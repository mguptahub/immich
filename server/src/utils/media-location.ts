import { StorageProvider } from 'src/enum';
import { EnvData } from 'src/repositories/config.repository';

/**
 * The virtual root used to namespace managed asset storage (originals, thumbnails,
 * encoded video, profile images, backups) when they live in an S3 bucket rather than
 * on disk. It is never read from or written to as an actual directory - object keys
 * are derived by stripping this prefix - so there is nothing to probe on disk for it.
 */
export const DEFAULT_S3_MEDIA_LOCATION = '/data';

/**
 * Resolves the media location root shared by {@link StorageCore} and
 * {@link StorageRepository}. Kept as a pure function of (env, existsSync) so both call
 * sites - and the storage repository's S3 path routing - independently derive the
 * exact same value without needing to depend on each other.
 */
export function detectMediaLocation(envData: EnvData, existsSync: (path: string) => boolean): string {
  if (envData.storage.mediaLocation) {
    return envData.storage.mediaLocation;
  }

  if (envData.storage.provider === StorageProvider.S3) {
    return DEFAULT_S3_MEDIA_LOCATION;
  }

  const candidates = ['/data', '/usr/src/app/upload'];
  const targets = candidates.filter((candidate) => existsSync(candidate));

  if (targets.length === 1) {
    return targets[0];
  }

  return '/usr/src/app/upload';
}
