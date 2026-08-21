import {
  CopyObjectCommand,
  DeleteObjectCommand,
  DeleteObjectsCommand,
  GetObjectCommand,
  HeadObjectCommand,
  ListObjectsV2Command,
  PutObjectCommand,
  S3Client,
} from '@aws-sdk/client-s3';
import { Upload } from '@aws-sdk/lib-storage';
import { Injectable } from '@nestjs/common';
import archiver from 'archiver';
import chokidar, { ChokidarOptions } from 'chokidar';
import { escapePath, glob, globStream } from 'fast-glob';
import { randomUUID } from 'node:crypto';
import {
  constants,
  createReadStream,
  createWriteStream,
  Dirent,
  existsSync,
  mkdirSync,
  ReadOptionsWithBuffer,
  watch,
} from 'node:fs';
import fs from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { PassThrough, Readable, Writable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { createGunzip, createGzip } from 'node:zlib';
import { CrawlOptionsDto, WalkOptionsDto } from 'src/dtos/library.dto';
import { StorageProvider } from 'src/enum';
import { ConfigRepository } from 'src/repositories/config.repository';
import { LoggingRepository } from 'src/repositories/logging.repository';
import { detectMediaLocation } from 'src/utils/media-location';
import { mimeTypes } from 'src/utils/mime-types';

export interface WatchEvents {
  onReady(): void;
  onAdd(path: string): void;
  onChange(path: string): void;
  onUnlink(path: string): void;
  onError(error: Error): void;
}

export interface ImmichReadStream {
  stream: Readable;
  type?: string;
  disposition?: string | string[];
  length?: number;
}

export interface ImmichZipStream extends ImmichReadStream {
  addFile: (inputPath: string, filename: string) => void;
  finalize: () => Promise<void>;
}

export interface DiskUsage {
  available: number;
  free: number;
  total: number;
}

export interface StorageStat {
  size: number;
  mtime: Date;
  mtimeMs: number;
  atime: Date;
  birthtimeMs: number;
  isFile(): boolean;
  isDirectory(): boolean;
  isCharacterDevice(): boolean;
}

@Injectable()
export class StorageRepository {
  private s3Client?: S3Client;

  constructor(
    private configRepository: ConfigRepository,
    private logger: LoggingRepository,
  ) {
    this.logger.setContext(StorageRepository.name);
  }

  realpath(filepath: string) {
    if (this.isManagedPath(filepath)) {
      // object storage has no symlinks to resolve
      return Promise.resolve(filepath);
    }
    return fs.realpath(filepath);
  }

  readdir(folder: string): Promise<string[]> {
    if (this.isManagedPath(folder)) {
      return this.s3Readdir(folder);
    }
    return fs.readdir(folder);
  }

  readdirWithTypes(folder: string): Promise<Dirent[]> {
    return fs.readdir(folder, { withFileTypes: true });
  }

  async copyFile(source: string, target: string) {
    if (this.isManagedPath(source)) {
      await this.s3CopyObject(source, target);
      return;
    }
    return fs.copyFile(source, target);
  }

  async stat(filepath: string): Promise<StorageStat> {
    if (this.isManagedPath(filepath)) {
      const head = await this.getS3Client().send(
        new HeadObjectCommand({ Bucket: this.s3Bucket, Key: this.toKey(filepath) }),
      );
      const mtime = head.LastModified ?? new Date(0);
      return {
        size: head.ContentLength ?? 0,
        mtime,
        mtimeMs: mtime.getTime(),
        atime: mtime,
        // S3 only exposes LastModified - there is no separate creation time to report
        birthtimeMs: mtime.getTime(),
        isFile: () => true,
        isDirectory: () => false,
        isCharacterDevice: () => false,
      };
    }
    return fs.stat(filepath);
  }

  async createFile(filepath: string, buffer: Buffer) {
    if (this.isManagedPath(filepath)) {
      await this.s3PutObject(filepath, buffer, { ifNotExists: true });
      return;
    }
    return fs.writeFile(filepath, buffer, { flag: 'wx' });
  }

  createWriteStream(filepath: string): Writable {
    if (this.isManagedPath(filepath)) {
      return this.s3CreateWriteStream(filepath);
    }
    return createWriteStream(filepath, { flags: 'w', flush: true });
  }

  async createOrOverwriteFile(filepath: string, buffer: Buffer) {
    if (this.isManagedPath(filepath)) {
      await this.s3PutObject(filepath, buffer);
      return;
    }
    return fs.writeFile(filepath, buffer, { flag: 'w' });
  }

  async overwriteFile(filepath: string, buffer: Buffer) {
    if (this.isManagedPath(filepath)) {
      await this.s3PutObject(filepath, buffer);
      return;
    }
    return fs.writeFile(filepath, buffer, { flag: 'r+' });
  }

  async rename(source: string, target: string) {
    if (this.isManagedPath(source)) {
      await this.s3CopyObject(source, target);
      await this.s3DeleteObject(source);
      return;
    }
    return fs.rename(source, target);
  }

  async utimes(filepath: string, atime: Date, mtime: Date) {
    if (this.isManagedPath(filepath)) {
      // S3 objects don't carry independently settable atime/mtime metadata
      return;
    }
    return fs.utimes(filepath, atime, mtime);
  }

  createZipStream(): ImmichZipStream {
    const archive = archiver('zip', { store: true });

    const addFile = (input: string, filename: string) => {
      if (this.isManagedPath(input)) {
        archive.append(this.createPlainReadStream(input), { name: filename, mode: 0o644 });
        return;
      }
      archive.file(input, { name: filename, mode: 0o644 });
    };

    const finalize = () => archive.finalize();

    return { stream: archive, addFile, finalize };
  }

  createGzip(): PassThrough {
    return createGzip();
  }

  createGunzip(): PassThrough {
    return createGunzip();
  }

  createPlainReadStream(filepath: string): Readable {
    if (this.isManagedPath(filepath)) {
      const passthrough = new PassThrough();
      this.getS3Client()
        .send(new GetObjectCommand({ Bucket: this.s3Bucket, Key: this.toKey(filepath) }))
        .then((response) => {
          const body = response.Body as Readable;
          body.on('error', (error) => passthrough.destroy(error));
          body.pipe(passthrough);
        })
        .catch((error: Error) => passthrough.destroy(error));
      return passthrough;
    }
    return createReadStream(filepath);
  }

  async createReadStream(
    filepath: string,
    mimeType?: string | null,
    range?: { start: number; end: number },
  ): Promise<ImmichReadStream> {
    if (this.isManagedPath(filepath)) {
      const response = await this.getS3Client().send(
        new GetObjectCommand({
          Bucket: this.s3Bucket,
          Key: this.toKey(filepath),
          Range: range ? `bytes=${range.start}-${range.end}` : undefined,
        }),
      );
      const passthrough = new PassThrough();
      const body = response.Body as Readable;
      body.on('error', (error) => passthrough.destroy(error));
      body.pipe(passthrough);
      return {
        stream: passthrough,
        length: response.ContentLength,
        type: mimeType || undefined,
      };
    }

    const { size } = await fs.stat(filepath);
    await fs.access(filepath, constants.R_OK);
    return {
      stream: createReadStream(filepath, range && { start: range.start, end: range.end }),
      length: range ? range.end - range.start + 1 : size,
      type: mimeType || undefined,
    };
  }

  async readFile(filepath: string, options?: ReadOptionsWithBuffer<Buffer>): Promise<Buffer> {
    if (this.isManagedPath(filepath)) {
      return this.s3ReadFile(filepath, options);
    }

    // read a slice
    if (options) {
      const file = await fs.open(filepath);
      try {
        const { buffer } = await file.read(options);
        return buffer as Buffer;
      } finally {
        await file.close();
      }
    }

    // read everything
    return fs.readFile(filepath);
  }

  async readJsonFile<T>(filepath: string): Promise<T> {
    const file = await fs.readFile(filepath, 'utf8');
    return JSON.parse(file) as T;
  }

  async checkFileExists(filepath: string, mode = constants.F_OK): Promise<boolean> {
    if (this.isManagedPath(filepath)) {
      try {
        await this.getS3Client().send(new HeadObjectCommand({ Bucket: this.s3Bucket, Key: this.toKey(filepath) }));
        return true;
      } catch {
        return false;
      }
    }
    try {
      await fs.access(filepath, mode);
      return true;
    } catch {
      return false;
    }
  }

  async unlink(file: string) {
    if (this.isManagedPath(file)) {
      // S3 deletes are idempotent - deleting a missing key is not an error
      await this.getS3Client().send(new DeleteObjectCommand({ Bucket: this.s3Bucket, Key: this.toKey(file) }));
      return;
    }
    try {
      await fs.unlink(file);
    } catch (error) {
      if ((error as NodeJS.ErrnoException)?.code === 'ENOENT') {
        this.logger.warn(`File ${file} does not exist.`);
      } else {
        throw error;
      }
    }
  }

  async unlinkDir(folder: string, options: { recursive?: boolean; force?: boolean }) {
    if (this.isManagedPath(folder)) {
      await this.s3DeleteByPrefix(this.toKey(folder));
      return;
    }
    await fs.rm(folder, { ...options, maxRetries: 5, retryDelay: 100 });
  }

  async removeEmptyDirs(directory: string, self: boolean = false) {
    // lstat does not follow symlinks (in contrast to stat)
    const stats = await fs.lstat(directory);
    if (!stats.isDirectory()) {
      return;
    }

    const files = await fs.readdir(directory);
    await Promise.all(files.map((file) => this.removeEmptyDirs(path.join(directory, file), true)));

    if (self) {
      const updated = await fs.readdir(directory);
      if (updated.length === 0) {
        try {
          await fs.rmdir(directory);
        } catch (error: Error | any) {
          if (error.code !== 'ENOTEMPTY') {
            this.logger.warn(`Attempted to remove directory, but failed: ${error}`);
          }
        }
      }
    }
  }

  mkdirSync(filepath: string): void {
    if (!existsSync(filepath)) {
      mkdirSync(filepath, { recursive: true });
    }
  }

  existsSync(filepath: string) {
    return existsSync(filepath);
  }

  async checkDiskUsage(folder: string): Promise<DiskUsage> {
    if (this.isManagedPath(folder)) {
      // S3-backed storage has no fixed quota to report; treat it as unbounded.
      return { available: Number.MAX_SAFE_INTEGER, free: Number.MAX_SAFE_INTEGER, total: Number.MAX_SAFE_INTEGER };
    }
    const stats = await fs.statfs(folder);
    return {
      available: stats.bavail * stats.bsize,
      free: stats.bfree * stats.bsize,
      total: stats.blocks * stats.bsize,
    };
  }

  crawl(crawlOptions: CrawlOptionsDto): Promise<string[]> {
    const { pathsToCrawl, exclusionPatterns, includeHidden } = crawlOptions;
    if (pathsToCrawl.length === 0) {
      return Promise.resolve([]);
    }

    const globbedPaths = pathsToCrawl.map((path) => this.asGlob(path));

    return glob(globbedPaths, {
      absolute: true,
      caseSensitiveMatch: false,
      onlyFiles: true,
      dot: includeHidden,
      ignore: exclusionPatterns,
    });
  }

  async *walk(walkOptions: WalkOptionsDto): AsyncGenerator<string[]> {
    const { pathsToCrawl, exclusionPatterns, includeHidden } = walkOptions;
    if (pathsToCrawl.length === 0) {
      async function* emptyGenerator() {}
      return emptyGenerator();
    }

    const globbedPaths = pathsToCrawl.map((path) => this.asGlob(path));

    const stream = globStream(globbedPaths, {
      absolute: true,
      caseSensitiveMatch: false,
      onlyFiles: true,
      dot: includeHidden,
      ignore: exclusionPatterns,
    });

    let batch: string[] = [];
    for await (const value of stream) {
      batch.push(value.toString());
      if (batch.length === walkOptions.take) {
        yield batch;
        batch = [];
      }
    }

    if (batch.length > 0) {
      yield batch;
    }
  }

  watch(paths: string[], options: ChokidarOptions, events: Partial<WatchEvents>) {
    const watcher = chokidar.watch(paths, options);

    watcher.on('ready', () => events.onReady?.());
    watcher.on('add', (path) => events.onAdd?.(path));
    watcher.on('change', (path) => events.onChange?.(path));
    watcher.on('unlink', (path) => events.onUnlink?.(path));
    watcher.on('error', (error) => events.onError?.(error as Error));

    return () => watcher.close();
  }

  watchDir = watch; // Native fs.watch without chokidar overhead

  /**
   * Materializes a managed (S3) file to a local temp path for the duration of `fn`, so
   * native tools that require a real filesystem path (sharp, exiftool, ffmpeg) can read
   * it. A no-op passthrough to `fn(filepath)` for local storage or non-managed paths.
   */
  async materializeReadPath<T>(filepath: string, fn: (localPath: string) => Promise<T>): Promise<T> {
    if (!this.isManagedPath(filepath)) {
      return fn(filepath);
    }

    const tempPath = this.getTempPath(filepath);
    await pipeline(this.createPlainReadStream(filepath), createWriteStream(tempPath));
    try {
      return await fn(tempPath);
    } finally {
      await fs.rm(tempPath, { force: true });
    }
  }

  /**
   * Gives `fn` a local temp path to write to (seeded with the existing object's contents,
   * if any) and uploads the result back to the managed (S3) key once `fn` resolves. A
   * no-op passthrough to `fn(filepath)` for local storage or non-managed paths.
   */
  async materializeWritePath<T>(filepath: string, fn: (localPath: string) => Promise<T>): Promise<T> {
    if (!this.isManagedPath(filepath)) {
      return fn(filepath);
    }

    const tempPath = this.getTempPath(filepath);
    if (await this.checkFileExists(filepath)) {
      await pipeline(this.createPlainReadStream(filepath), createWriteStream(tempPath));
    }
    try {
      const result = await fn(tempPath);
      await pipeline(createReadStream(tempPath), this.createWriteStream(filepath));
      return result;
    } finally {
      await fs.rm(tempPath, { force: true });
    }
  }

  private getTempPath(filepath: string): string {
    return path.join(tmpdir(), `immich-${randomUUID()}${path.extname(filepath)}`);
  }

  private asGlob(pathToCrawl: string): string {
    const escapedPath = escapePath(pathToCrawl).replaceAll('"', '["]').replaceAll("'", "[']").replaceAll('`', '[`]');
    const extensions = `*{${mimeTypes.getSupportedFileExtensions().join(',')}}`;
    return `${escapedPath}/**/${extensions}`;
  }

  // -- S3 storage provider --------------------------------------------------

  private isS3Enabled(): boolean {
    return this.configRepository.getEnv().storage.provider === StorageProvider.S3;
  }

  private getManagedRoot(): string {
    const envData = this.configRepository.getEnv();
    return detectMediaLocation(envData, existsSync);
  }

  /** Whether `filepath` falls under the managed storage root and should be routed to S3. */
  isManagedPath(filepath: string): boolean {
    if (!this.isS3Enabled()) {
      return false;
    }
    const relative = path.relative(this.getManagedRoot(), filepath);
    return relative === '' || (!relative.startsWith('..') && !path.isAbsolute(relative));
  }

  private get s3Config() {
    const config = this.configRepository.getEnv().storage.s3;
    if (!config) {
      throw new Error('S3 storage is not configured');
    }
    return config;
  }

  private get s3Bucket(): string {
    return this.s3Config.bucket;
  }

  private toKey(filepath: string): string {
    const relative = path.relative(this.getManagedRoot(), filepath).split(path.sep).join('/');
    const prefix = this.s3Config.keyPrefix?.replaceAll(/^\/+|\/+$/g, '');
    return prefix ? `${prefix}/${relative}` : relative;
  }

  private getS3Client(): S3Client {
    if (!this.s3Client) {
      const { region, endpoint, accessKeyId, secretAccessKey, forcePathStyle } = this.s3Config;
      this.s3Client = new S3Client({
        region: region || 'us-east-1',
        endpoint,
        forcePathStyle,
        credentials: accessKeyId && secretAccessKey ? { accessKeyId, secretAccessKey } : undefined,
      });
    }
    return this.s3Client;
  }

  private isS3PreconditionFailed(error: unknown): boolean {
    // Duck-typed rather than `instanceof S3ServiceException` since not every S3-compatible
    // provider's SDK error deserializer produces that exact class.
    if (!error || typeof error !== 'object') {
      return false;
    }
    const { name, $metadata } = error as { name?: string; $metadata?: { httpStatusCode?: number } };
    return name === 'PreconditionFailed' || $metadata?.httpStatusCode === 412;
  }

  private async s3PutObject(filepath: string, body: Buffer, options: { ifNotExists?: boolean } = {}) {
    try {
      await this.getS3Client().send(
        new PutObjectCommand({
          Bucket: this.s3Bucket,
          Key: this.toKey(filepath),
          Body: body,
          ...(options.ifNotExists && { IfNoneMatch: '*' }),
        }),
      );
    } catch (error) {
      if (options.ifNotExists && this.isS3PreconditionFailed(error)) {
        const eexist = new Error(`File already exists: ${filepath}`) as NodeJS.ErrnoException;
        eexist.code = 'EEXIST';
        throw eexist;
      }
      throw error;
    }
  }

  private s3CreateWriteStream(filepath: string): Writable {
    const client = this.getS3Client();
    const passthrough = new PassThrough();
    const uploadDone = new Upload({
      client,
      params: { Bucket: this.s3Bucket, Key: this.toKey(filepath), Body: passthrough },
    }).done();
    // avoid an unhandled rejection if the writable is destroyed before `final` observes the error
    uploadDone.catch(() => {});

    return new Writable({
      write: (chunk, encoding, callback) => passthrough.write(chunk, encoding, callback),
      final: (callback) => {
        passthrough.end();
        uploadDone.then(() => callback()).catch((error: Error) => callback(error));
      },
      destroy: (error, callback) => {
        passthrough.destroy(error ?? undefined);
        callback(error);
      },
    });
  }

  private async s3ReadFile(filepath: string, options?: ReadOptionsWithBuffer<Buffer>): Promise<Buffer> {
    const range = options
      ? `bytes=${Number(options.position)}-${Number(options.position) + options.length! - 1}`
      : undefined;
    const response = await this.getS3Client().send(
      new GetObjectCommand({ Bucket: this.s3Bucket, Key: this.toKey(filepath), Range: range }),
    );
    const chunks: Buffer[] = [];
    for await (const chunk of response.Body as Readable) {
      chunks.push(chunk as Buffer);
    }
    const data = Buffer.concat(chunks);
    if (!options?.buffer) {
      return data;
    }
    data.copy(options.buffer, options.offset ?? 0, 0, options.length);
    return options.buffer;
  }

  private async s3CopyObject(source: string, target: string) {
    const encodedKey = this.toKey(source)
      .split('/')
      .map((segment) => encodeURIComponent(segment))
      .join('/');
    await this.getS3Client().send(
      new CopyObjectCommand({
        Bucket: this.s3Bucket,
        CopySource: `${this.s3Bucket}/${encodedKey}`,
        Key: this.toKey(target),
      }),
    );
  }

  private async s3DeleteObject(filepath: string) {
    await this.getS3Client().send(new DeleteObjectCommand({ Bucket: this.s3Bucket, Key: this.toKey(filepath) }));
  }

  private async s3DeleteByPrefix(prefix: string) {
    const client = this.getS3Client();
    const normalizedPrefix = prefix.endsWith('/') ? prefix : `${prefix}/`;
    let continuationToken: string | undefined;
    do {
      const listed = await client.send(
        new ListObjectsV2Command({
          Bucket: this.s3Bucket,
          Prefix: normalizedPrefix,
          ContinuationToken: continuationToken,
        }),
      );
      const keys = (listed.Contents ?? []).map((object) => object.Key).filter((key): key is string => !!key);
      if (keys.length > 0) {
        await client.send(
          new DeleteObjectsCommand({ Bucket: this.s3Bucket, Delete: { Objects: keys.map((Key) => ({ Key })) } }),
        );
      }
      continuationToken = listed.IsTruncated ? listed.NextContinuationToken : undefined;
    } while (continuationToken);
  }

  private async s3Readdir(folder: string): Promise<string[]> {
    const client = this.getS3Client();
    const prefix = this.toKey(folder);
    const normalizedPrefix = prefix === '' ? '' : prefix.endsWith('/') ? prefix : `${prefix}/`;
    const names = new Set<string>();
    let continuationToken: string | undefined;
    do {
      const listed = await client.send(
        new ListObjectsV2Command({
          Bucket: this.s3Bucket,
          Prefix: normalizedPrefix,
          Delimiter: '/',
          ContinuationToken: continuationToken,
        }),
      );
      for (const object of listed.Contents ?? []) {
        if (object.Key) {
          names.add(object.Key.slice(normalizedPrefix.length));
        }
      }
      for (const commonPrefix of listed.CommonPrefixes ?? []) {
        if (commonPrefix.Prefix) {
          names.add(commonPrefix.Prefix.slice(normalizedPrefix.length).replace(/\/$/, ''));
        }
      }
      continuationToken = listed.IsTruncated ? listed.NextContinuationToken : undefined;
    } while (continuationToken);
    return [...names];
  }
}
