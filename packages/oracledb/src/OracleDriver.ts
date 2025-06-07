import {
  type AnyEntity,
  type Configuration,
  type ConnectionType,
  type EntityDictionary,
  type LoggingOptions,
  type NativeInsertUpdateManyOptions,
  QueryFlag,
  type QueryResult,
  type RequiredEntityData,
  type Transaction,
} from '@mikro-orm/core';
import { AbstractSqlDriver, type SqlEntityManager } from '@mikro-orm/knex';
import { OracleConnection } from './OracleConnection.js';
import { OracleQueryBuilder } from './OracleQueryBuilder.js';
import { OraclePlatform } from './OraclePlatform.js';

export class OracleDriver extends AbstractSqlDriver<OracleConnection> {

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

  override async nativeInsertMany<T extends object>(entityName: string, data: EntityDictionary<T>[], options: NativeInsertUpdateManyOptions<T> = {}, transform?: (sql: string) => string): Promise<QueryResult<T>> {
    const qb = this.createQueryBuilder<T>(entityName, options.ctx, 'write', options.convertCustomTypes);
    return qb.insert(data as RequiredEntityData<T>[]).execute('run');
  }

}
