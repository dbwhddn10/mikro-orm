import { type AnyEntity, type Dictionary, type EntityMetadata, isRaw, raw, Utils } from '@mikro-orm/core';
import { type NativeQueryBuilder, type Field, QueryBuilder, QueryType } from '@mikro-orm/knex';

export class OracleQueryBuilder<
  Entity extends object = AnyEntity,
  RootAlias extends string = never,
  Hint extends string = never,
  Context extends object = never,
> extends QueryBuilder<Entity, RootAlias, Hint, Context> {

  protected override processReturningStatement(qb: NativeQueryBuilder, meta?: EntityMetadata, data?: Dictionary, returning?: Field<any>[]): void {
    if (!meta || !data) {
      return;
    }

    const arr = Utils.asArray(data);

    // always respect explicit returning hint
    if (returning && returning.length > 0) {
      qb.returning(returning.map(field => this.helper.mapper(field as string, this.type)));

      return;
    }

    if (this.type === QueryType.INSERT) {
      const returningProps = meta.hydrateProps
        .filter(prop => prop.returning || (prop.persist !== false && ((prop.primary && prop.autoincrement) || prop.defaultRaw)))
        .filter(prop => !(prop.fieldNames[0] in arr[0]));

      if (returningProps.length > 0) {
        qb.returning(returningProps.map(prop => [prop.fieldNames[0], prop.runtimeType]));
      }

      return;
    }

    if (this.type === QueryType.UPDATE) {
      const returningProps = meta.hydrateProps.filter(prop => prop.fieldNames && isRaw(arr[0][prop.fieldNames[0]]));

      if (returningProps.length > 0) {
        qb.returning(returningProps.flatMap(prop => {
          if (prop.hasConvertToJSValueSQL) {
            const aliased = this.platform.quoteIdentifier(prop.fieldNames[0]);
            const sql = prop.customType!.convertToJSValueSQL!(aliased, this.platform) + ' as ' + this.platform.quoteIdentifier(prop.fieldNames[0]);
            return [raw(sql)];
          }
          return prop.fieldNames;
        }) as any);
      }
    }
  }

}
