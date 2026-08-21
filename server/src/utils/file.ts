import { HttpException, NotFoundException, StreamableFile } from '@nestjs/common';
import { NextFunction, Request, Response } from 'express';
import { access, constants } from 'node:fs/promises';
import { basename, extname } from 'node:path';
import { pipeline } from 'node:stream/promises';
import { promisify } from 'node:util';
import { CacheControl } from 'src/enum';
import { LoggingRepository } from 'src/repositories/logging.repository';
import { ImmichReadStream, StorageRepository } from 'src/repositories/storage.repository';
import { isConnectionAborted } from 'src/utils/misc';

export function getFileNameWithoutExtension(path: string): string {
  return basename(path, getFilenameExtension(path));
}

export function getFilenameExtension(path: string) {
  const extension = extname(path);
  if (!extension && path.startsWith('.') && !path.includes('.', 1)) {
    return path;
  }
  return extension;
}

export function getLivePhotoMotionFilename(stillName: string, motionName: string) {
  return getFileNameWithoutExtension(stillName) + getFilenameExtension(motionName);
}

export class ImmichFileResponse {
  public readonly path!: string;
  public readonly contentType!: string;
  public readonly cacheControl!: CacheControl;
  public readonly fileName?: string;

  constructor(response: ImmichFileResponse) {
    Object.assign(this, response);
  }
}
type SendFile = Parameters<Response['sendFile']>;
type SendFileOptions = SendFile[1];

const cacheControlHeaders: Record<CacheControl, string | null> = {
  [CacheControl.PrivateWithCache]:
    'private, max-age=86400, no-transform, stale-while-revalidate=2592000, stale-if-error=2592000',
  [CacheControl.PrivateWithoutCache]: 'private, no-cache, no-transform',
  [CacheControl.None]: null, // falsy value to prevent adding Cache-Control header
};

type ByteRange = { start: number; end: number };

/** Parses a single-range `Range: bytes=start-end` header; returns null when absent, malformed, or unsatisfiable. */
const parseRange = (rangeHeader: string | undefined, totalSize: number): ByteRange | null => {
  if (!rangeHeader?.startsWith('bytes=') || rangeHeader.includes(',')) {
    return null;
  }

  const [startText, endText] = rangeHeader.slice('bytes='.length).split('-', 2);
  const start = startText ? Number(startText) : 0;
  const end = endText ? Number(endText) : totalSize - 1;

  if (!Number.isSafeInteger(start) || !Number.isSafeInteger(end) || start < 0 || start > end || end >= totalSize) {
    return null;
  }

  return { start, end };
};

/**
 * Streams an S3-managed file to the response, since Express's `res.sendFile` only
 * understands local paths. Handles byte-range requests (206) for video scrubbing.
 */
const sendManagedFile = async (
  res: Response,
  req: Request,
  file: ImmichFileResponse,
  storageRepository: StorageRepository,
): Promise<void> => {
  const stat = await storageRepository.stat(file.path);
  const range = parseRange(req.headers.range, stat.size);

  res.header('Accept-Ranges', 'bytes');

  const { stream, length } = await storageRepository.createReadStream(file.path, file.contentType, range ?? undefined);

  if (range) {
    res.status(206);
    res.header('Content-Range', `bytes ${range.start}-${range.end}/${stat.size}`);
  }
  res.header('Content-Length', String(length ?? stat.size));

  await pipeline(stream, res);
};

export const sendFile = async (
  res: Response,
  req: Request,
  next: NextFunction,
  handler: () => Promise<ImmichFileResponse> | ImmichFileResponse,
  storageRepository: StorageRepository,
  logger: LoggingRepository,
): Promise<void> => {
  // promisified version of 'res.sendFile' for cleaner async handling
  const _sendFile = (path: string, options: SendFileOptions) =>
    promisify<string, SendFileOptions>(res.sendFile).bind(res)(path, options);

  try {
    const file = await handler();

    const cacheControlHeader = cacheControlHeaders[file.cacheControl];
    if (cacheControlHeader) {
      // set the header to Cache-Control
      res.set('Cache-Control', cacheControlHeader);
    }

    res.header('Content-Type', file.contentType);
    if (file.fileName) {
      res.header('Content-Disposition', `inline; filename*=UTF-8''${encodeURIComponent(file.fileName)}`);
    }

    if (storageRepository.isManagedPath(file.path)) {
      return await sendManagedFile(res, req, file, storageRepository);
    }

    await access(file.path, constants.R_OK);
    return await _sendFile(file.path, { dotfiles: 'allow' });
  } catch (error: Error | any) {
    // ignore client-closed connection
    if (isConnectionAborted(error) || res.headersSent) {
      return;
    }

    // log non-http errors
    if (!(error instanceof HttpException)) {
      logger.error(`Unable to send file: ${error}`, error.stack);
    }

    next(new NotFoundException());
  }
};

export const asStreamableFile = ({ stream, type, disposition, length }: ImmichReadStream) => {
  return new StreamableFile(stream, { type, disposition, length });
};
