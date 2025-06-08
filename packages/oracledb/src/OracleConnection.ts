import { readFile } from 'node:fs/promises';
import {
  AbstractSqlConnection,
  type AnyEntity,
  type ConnectionConfig,
  type Dictionary,
  type EntityData,
  type LoggingOptions,
  NativeQueryBuilder,
  OracleDialect,
  type QueryResult,
  RawQueryFragment,
  type Transaction,
  Utils,
} from '@mikro-orm/knex';
// import { OracleDialect } from 'kysely-oracledb';
import { CompiledQuery } from 'kysely';
import oracledb, { type PoolAttributes } from 'oracledb';

export class OracleConnection extends AbstractSqlConnection {

  override async createKyselyDialect(overrides: PoolAttributes) {
    const options = this.mapOptions(overrides);
    const password = options.password as ConnectionConfig['password'];
    const onCreateConnection = this.options.onCreateConnection ?? this.config.get('onCreateConnection');

    const pool = await oracledb.createPool({
      ...options,
      password: typeof password === 'function' ? await password() : password,
      sessionCallback: onCreateConnection,
    });

    // FIXME ideally this would be set locally, we could hack it via postprocessing, based on `res.metaData`
    //   which we can use to find what columns are CLOBs, and map them to strings afterwards
    oracledb.fetchAsString = [oracledb.DB_TYPE_CLOB, oracledb.DB_TYPE_NUMBER];

    return new OracleDialect({ pool });
  }

  mapOptions(overrides: PoolAttributes): PoolAttributes {
    const ret = { ...this.getConnectionOptions() } as PoolAttributes;
    const dbName = this.config.get('dbName');
    const pool = this.config.get('pool');
    ret.poolMin = pool?.min;
    ret.poolMax = pool?.max;
    ret.poolTimeout = pool?.idleTimeoutMillis;
    const user = this.config.get('user', dbName)!;
    ret.user = user.startsWith('"') || user === 'system' ? user : this.platform.quoteIdentifier(user);
    ret.connectionString = this.config.getClientUrl();
    Reflect.deleteProperty(ret, 'database');

    return Utils.mergeConfig(ret, overrides);
  }

  override async execute<T extends QueryResult | EntityData<AnyEntity> | EntityData<AnyEntity>[] = EntityData<AnyEntity>[]>(query: string | NativeQueryBuilder | RawQueryFragment, params: readonly unknown[] = [], method: 'all' | 'get' | 'run' = 'all', ctx?: Transaction, loggerContext?: LoggingOptions): Promise<T> {
    await this.ensureConnection();

    // console.log('execute', query, params, new Error().stack);

    if (query instanceof NativeQueryBuilder) {
      query = query.toRaw();
    }

    if (query instanceof RawQueryFragment) {
      params = query.params;
      query = query.sql;
    }

    if (query.endsWith(';') && !query.endsWith('end;')) {
      query = query.slice(0, -1);
    }

    const last = params[params.length - 1];

    if (!Utils.isObject(last) || !('__outBindings' in last) || !last.__outBindings) {
      return super.execute(query, params, method, ctx, loggerContext);
    }

    delete last.__outBindings;
    query = this.config.get('onQuery')(query, params);
    (params as unknown[]).pop(); // FIXME maybe a bit too hackish?
    const formatted = this.platform.formatQuery(query, params);
    const sql = this.getSql(query, formatted, loggerContext);

    return this.executeQuery<T>(sql, async () => {
      const compiled = CompiledQuery.raw(formatted, last as unknown[]);

      if (ctx) {
        const res = await ctx.executeQuery(compiled);
        return this.transformRawResult<T>(res, method);
      }

      const res = await this.client.executeQuery({
        ...compiled,
        autoCommit: true,
      } as CompiledQuery);
      return this.transformRawResult<T>(res, method);
    }, { query, params, ...loggerContext });
  }

  override async loadFile(path: string): Promise<void> {
    const buf = await readFile(path);
    const sql = buf.toString();
    const lines = sql.split('\n').filter(i => i.trim());

    for (let line of lines) {
      if (line.startsWith('--')) {
        continue;
      }

      if (line.endsWith(';') && !line.endsWith('end;')) {
        line = line.slice(0, -1);
      }

      const raw = CompiledQuery.raw(line);
      const now = Date.now();

      try {
        await this.client.executeQuery(raw);
      } catch (e) {
        this.logQuery(line, { took: Date.now() - now, level: 'error', query: line });
        throw e;
      }
    }
  }

  protected override transformRawResult<T>(res: any, method: 'all' | 'get' | 'run'): T {
    // console.log('transformRawResult', res, method, res.outBinds);
    if (method === 'get') {
      return res.rows[0];
    }

    if (method === 'all') {
      return res.rows;
    }

    const rowCount = res.rows.length;
    const hasEmptyCount = (rowCount === 1) && ('' in res.rows[0]);
    const emptyRow = hasEmptyCount && Number(res.rows[0]['']);

    if (res.numAffectedRows > 0n && res.outBinds) {
      const keys = Object.keys(res.outBinds);
      const rows: Dictionary[] = [];
      res.rows = rows;

      for (let i = 0; i < res.numAffectedRows; i++) {
        const o: Dictionary = {};

        for (const key of keys) {
          o[key.replace(/^out_/, '')] = res.outBinds[key][i];
        }

        rows.push(o);
      }
    } else if (res.outBinds) {
      const keys = Object.keys(res.outBinds);
      const rows: Dictionary[] = [];

      for (const key of keys) {
        const [k, i] = key.split('__');
        rows[+i] ??= {};
        rows[+i][k] = res.outBinds[key];
      }

      res.rows = rows;
    }

    return {
      affectedRows: hasEmptyCount ? emptyRow : Number(res.numAffectedRows),
      row: res.rows[0],
      rows: res.rows,
    } as unknown as T;
  }

}
