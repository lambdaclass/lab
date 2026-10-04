/**
 * Prefix a path under `public/` with the app's base path (Vite `base`), so the lab also
 * works when served under a sub-path such as `/lab/`. With the default base `/` the path is
 * returned unchanged.
 */
export function publicPath(path: string): string {
  return `${import.meta.env.BASE_URL.replace(/\/$/, '')}${path}`;
}
