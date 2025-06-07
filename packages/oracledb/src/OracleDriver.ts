import {
  type AnyEntity,
  type Configuration,
  type ConnectionType,
  type Dictionary,
  type EntityDictionary,
  type EntityKey,
  type FilterQuery,
  isRaw,
  type LoggingOptions,
  type NativeInsertUpdateManyOptions,
  QueryFlag,
  type QueryResult,
  type RequiredEntityData,
  type Transaction,
  type UpsertManyOptions,
  Utils,
} from '@mikro-orm/core';
import { AbstractSqlDriver, type SqlEntityManager } from '@mikro-orm/knex';
import { OracleConnection } from './OracleConnection.js';
import { OracleQueryBuilder } from './OracleQueryBuilder.js';
import { OraclePlatform } from './OraclePlatform.js';

export class OracleDriver extends AbstractSqlDriver<OracleConnection, OraclePlatform> {

  constructor(config: Configuration) {
    super(config, new OraclePlatform(), OracleConnection, ['kysely', 'oracledb']);
  }

  override createQueryBuilder<T extends AnyEntity<T>>(entityName: string, ctx?: Transaction, preferredConnectionType?: ConnectionType, convertCustomTypes?: boolean, loggerContext?: LoggingOptions, alias?: string, em?: SqlEntityManager): OracleQueryBuilder<T, any, any, any> {
    // do not compute the connectionType if EM is provided as it will be computed from it in the QB later on
    const connectionType = em ? preferredConnectionType : this.resolveConnectionType({ ctx, connectionType: preferredConnectionType });
    const qb = new OracleQueryBuilder<T, any, any, any>(entityName, this.metadata, this, ctx, alias, connectionType, em, loggerContext);

    if (!convertCustomTypes) {
      qb.unsetFlag(QueryFlag.CONVERT_CUSTOM_TYPES);
    }

    return qb;
  }

  override async nativeInsertMany<T extends object>(entityName: string, data: EntityDictionary<T>[], options: NativeInsertUpdateManyOptions<T> = {}): Promise<QueryResult<T>> {
    options.processCollections ??= true;
    options.convertCustomTypes ??= true;
    const meta = this.metadata.get<T>(entityName);
    const qb = this.createQueryBuilder<T>(entityName, options.ctx, 'write', options.convertCustomTypes).withSchema(this.getSchemaName(meta, options));

    return qb.insert(data as RequiredEntityData<T>[]).execute('run');
  }

  override async nativeUpdateMany<T extends object>(entityName: string, where: FilterQuery<T>[], data: EntityDictionary<T>[], options: NativeInsertUpdateManyOptions<T> & UpsertManyOptions<T> = {}): Promise<QueryResult<T>> {
    const meta = this.metadata.get<T>(entityName);
    const returning = new Set<EntityKey<T>>();
    const into: string[] = [];
    const outBindings: Dictionary = {};
    Object.defineProperty(outBindings, '__outBindings', { value: true, writable: true, configurable: true, enumerable: false });

    for (const row of data) {
      for (const k of Utils.keys(row)) {
        if (isRaw(row[k])) {
          returning.add(k);
        }
      }
    }

    // reload generated columns and version fields
    meta.props
      .filter(prop => prop.generated || prop.version || prop.primary)
      .forEach(prop => returning.add(prop.name));

    for (const propName of returning) {
      const prop = meta.properties[propName];
      into.push(`:${prop.fieldNames[0]}`);
      outBindings[prop.fieldNames[0]] = {
        dir: this.platform.mapToOracleType('out'),
        type: this.platform.mapToOracleType(prop.runtimeType),
      };
    }

    return super.nativeUpdateMany(entityName, where, data, options, (sql, params) => {
      if (into.length === 0) {
        return sql;
      }

      params.push(outBindings);

      return `${sql} into ${into.join(', ')}`;
    });
  }

}
