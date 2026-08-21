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
import { mockClient } from 'aws-sdk-client-mock';
import { Readable } from 'node:stream';
import { text } from 'node:stream/consumers';
import { StorageProvider } from 'src/enum';
import { ConfigRepository } from 'src/repositories/config.repository';
import { LoggingRepository } from 'src/repositories/logging.repository';
import { StorageRepository } from 'src/repositories/storage.repository';
import { automock } from 'test/utils';

const s3Mock = mockClient(S3Client);

const sdkBody = (contents: string) => Readable.from([Buffer.from(contents)]);

describe(`${StorageRepository.name} (S3)`, () => {
  let sut: StorageRepository;

  beforeEach(() => {
    s3Mock.reset();

    const configMock = automock(ConfigRepository, { strict: false });
    configMock.getEnv.mockReturnValue({
      storage: {
        provider: StorageProvider.S3,
        mediaLocation: '/data',
        s3: {
          bucket: 'test-bucket',
          region: 'us-east-1',
          forcePathStyle: false,
        },
      },
    } as any);

    sut = new StorageRepository(
      configMock,
      // eslint-disable-next-line no-sparse-arrays
      automock(LoggingRepository, { args: [, { getEnv: () => ({}) }], strict: false }),
    );
  });

  describe('isManagedPath', () => {
    it('routes paths under the media location to S3', () => {
      expect(sut.isManagedPath('/data/thumbs/user-1/ab/cd/asset.jpg')).toBe(true);
      expect(sut.isManagedPath('/data')).toBe(true);
    });

    it('does not route paths outside the media location (e.g. external libraries)', () => {
      expect(sut.isManagedPath('/mnt/photos/vacation.jpg')).toBe(false);
      expect(sut.isManagedPath('/data-other/file.jpg')).toBe(false);
    });
  });

  describe('createFile', () => {
    it('puts the object with an IfNoneMatch guard', async () => {
      s3Mock.on(PutObjectCommand).resolves({});

      await sut.createFile('/data/thumbs/a.jpg', Buffer.from('hello'));

      const calls = s3Mock.commandCalls(PutObjectCommand);
      expect(calls).toHaveLength(1);
      expect(calls[0].args[0].input).toMatchObject({
        Bucket: 'test-bucket',
        Key: 'thumbs/a.jpg',
        IfNoneMatch: '*',
      });
    });

    it('normalizes a PreconditionFailed response to an EEXIST error', async () => {
      class PreconditionFailed extends Error {
        name = 'PreconditionFailed';
        $metadata = { httpStatusCode: 412 };
      }
      s3Mock.on(PutObjectCommand).rejects(new PreconditionFailed('PreconditionFailed'));

      await expect(sut.createFile('/data/thumbs/a.jpg', Buffer.from('hello'))).rejects.toMatchObject({
        code: 'EEXIST',
      });
    });
  });

  describe('createOrOverwriteFile', () => {
    it('puts the object without a conditional guard', async () => {
      s3Mock.on(PutObjectCommand).resolves({});

      await sut.createOrOverwriteFile('/data/thumbs/a.jpg', Buffer.from('hello'));

      const calls = s3Mock.commandCalls(PutObjectCommand);
      expect(calls[0].args[0].input.IfNoneMatch).toBeUndefined();
    });
  });

  describe('readFile', () => {
    it('reads the full object', async () => {
      s3Mock.on(GetObjectCommand).resolves({ Body: sdkBody('hello world') as never });

      const result = await sut.readFile('/data/backups/db.sql.gz');

      expect(result.toString()).toBe('hello world');
    });

    it('reads a byte range into the provided buffer', async () => {
      s3Mock.on(GetObjectCommand).resolves({ Body: sdkBody('ell') as never });

      const buffer = Buffer.alloc(3);
      const result = await sut.readFile('/data/upload/video.mp4', { buffer, position: 1, length: 3 });

      expect(result.toString()).toBe('ell');
      const calls = s3Mock.commandCalls(GetObjectCommand);
      expect(calls[0].args[0].input.Range).toBe('bytes=1-3');
    });
  });

  describe('checkFileExists', () => {
    it('returns true when the head request succeeds', async () => {
      s3Mock.on(HeadObjectCommand).resolves({});
      await expect(sut.checkFileExists('/data/thumbs/a.jpg')).resolves.toBe(true);
    });

    it('returns false when the object is missing', async () => {
      s3Mock.on(HeadObjectCommand).rejects(new Error('NotFound'));
      await expect(sut.checkFileExists('/data/thumbs/a.jpg')).resolves.toBe(false);
    });
  });

  describe('unlink', () => {
    it('deletes the object', async () => {
      s3Mock.on(DeleteObjectCommand).resolves({});

      await sut.unlink('/data/thumbs/a.jpg');

      const calls = s3Mock.commandCalls(DeleteObjectCommand);
      expect(calls[0].args[0].input).toMatchObject({ Bucket: 'test-bucket', Key: 'thumbs/a.jpg' });
    });
  });

  describe('rename', () => {
    it('copies to the new key and deletes the old one', async () => {
      s3Mock.on(CopyObjectCommand).resolves({});
      s3Mock.on(DeleteObjectCommand).resolves({});

      await sut.rename('/data/upload/a.jpg', '/data/upload/b.jpg');

      const copyCalls = s3Mock.commandCalls(CopyObjectCommand);
      expect(copyCalls[0].args[0].input).toMatchObject({
        Bucket: 'test-bucket',
        CopySource: 'test-bucket/upload/a.jpg',
        Key: 'upload/b.jpg',
      });
      const deleteCalls = s3Mock.commandCalls(DeleteObjectCommand);
      expect(deleteCalls[0].args[0].input).toMatchObject({ Key: 'upload/a.jpg' });
    });
  });

  describe('stat', () => {
    it('maps HeadObject to a StorageStat', async () => {
      const lastModified = new Date('2024-01-01T00:00:00Z');
      s3Mock.on(HeadObjectCommand).resolves({ ContentLength: 42, LastModified: lastModified });

      const stat = await sut.stat('/data/thumbs/a.jpg');

      expect(stat.size).toBe(42);
      expect(stat.mtime).toEqual(lastModified);
      expect(stat.isFile()).toBe(true);
      expect(stat.isDirectory()).toBe(false);
    });
  });

  describe('readdir', () => {
    it('merges CommonPrefixes and Contents into immediate child names', async () => {
      s3Mock.on(ListObjectsV2Command).resolves({
        CommonPrefixes: [{ Prefix: 'thumbs/user-1/' }],
        Contents: [{ Key: 'thumbs/.immich' }],
        IsTruncated: false,
      });

      const names = await sut.readdir('/data/thumbs');

      expect(names.sort()).toEqual(['.immich', 'user-1']);
    });
  });

  describe('unlinkDir', () => {
    it('lists and batch-deletes every object under the prefix', async () => {
      s3Mock
        .on(ListObjectsV2Command)
        .resolves({ Contents: [{ Key: 'encoded-video/user-1/session-1/init.mp4' }], IsTruncated: false });
      s3Mock.on(DeleteObjectsCommand).resolves({});

      await sut.unlinkDir('/data/encoded-video/user-1/session-1', { recursive: true, force: true });

      const calls = s3Mock.commandCalls(DeleteObjectsCommand);
      expect(calls[0].args[0].input.Delete?.Objects).toEqual([{ Key: 'encoded-video/user-1/session-1/init.mp4' }]);
    });
  });

  describe('createWriteStream', () => {
    it('uploads the written bytes and only finishes once the upload completes', async () => {
      s3Mock.on(PutObjectCommand).resolves({});

      const stream = sut.createWriteStream('/data/backups/db.sql.gz');
      await new Promise<void>((resolve, reject) => {
        stream.on('finish', resolve);
        stream.on('error', reject);
        stream.end(Buffer.from('backup contents'));
      });

      const calls = s3Mock.commandCalls(PutObjectCommand);
      expect(calls).toHaveLength(1);
      expect(calls[0].args[0].input.Key).toBe('backups/db.sql.gz');
    });
  });

  describe('createPlainReadStream / createReadStream', () => {
    it('streams the object synchronously via createPlainReadStream', async () => {
      s3Mock.on(GetObjectCommand).resolves({ Body: sdkBody('stream me') as never });

      const stream = sut.createPlainReadStream('/data/backups/db.sql.gz');

      await expect(text(stream)).resolves.toBe('stream me');
    });

    it('supports a byte range via createReadStream', async () => {
      s3Mock.on(GetObjectCommand).resolves({ Body: sdkBody('range') as never, ContentLength: 5 });

      const { stream, length } = await sut.createReadStream('/data/upload/video.mp4', 'video/mp4', {
        start: 0,
        end: 4,
      });

      expect(length).toBe(5);
      await expect(text(stream)).resolves.toBe('range');
      const calls = s3Mock.commandCalls(GetObjectCommand);
      expect(calls[0].args[0].input.Range).toBe('bytes=0-4');
    });
  });
});
