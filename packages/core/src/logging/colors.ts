const bool = (v?: string) => v && ['true', 't', '1'].includes(v.toLowerCase());
const enabled = () => {
  const keys = ['FORCE_COLOR', 'NO_COLOR', 'MIKRO_ORM_COLORS', 'MIKRO_ORM_NO_COLOR'];

  for (const key of keys) {
    if (process.env[key] != null) {
      return bool(process.env[key]);
    }
  }

  return false;
};
const wrap = (fn: (text: string) => string) => (text: string) => enabled() ? fn(text) : text;

/** @internal */
export const colors = {
  red: wrap((text: string) => `\x1B[31m${text}\x1B[39m`),
  green: wrap((text: string) => `\x1B[32m${text}\x1B[39m`),
  yellow: wrap((text: string) => `\x1B[33m${text}\x1B[39m`),
  grey: wrap((text: string) => `\x1B[90m${text}\x1B[39m`),
  cyan: wrap((text: string) => `\x1B[36m${text}\x1B[39m`),
  enabled,
};
