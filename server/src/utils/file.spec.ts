import { Readable, Writable } from 'node:stream';
import { CacheControl } from 'src/enum';
import { LoggingRepository } from 'src/repositories/logging.repository';
import { ImmichFileResponse, sendFile } from 'src/utils/file';
import { automock, mockStorageRepository } from 'test/utils';

const makeRes = () => {
  const chunks: Buffer[] = [];
  const writable = new Writable({
    write(chunk, _encoding, callback) {
      chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
      callback();
    },
  }) as Writable & {
    header: (name: string, value: string) => void;
    set: (name: string, value: string) => void;
    status: (code: number) => void;
    headersSent: boolean;
    statusCode?: number;
    headers: Record<string, string>;
  };

  writable.headers = {};
  writable.headersSent = false;
  writable.header = (name: string, value: string) => {
    writable.headers[name] = value;
  };
  writable.set = writable.header;
  writable.status = (code: number) => {
    writable.statusCode = code;
  };

  return { res: writable, body: () => Buffer.concat(chunks).toString() };
};

describe('sendFile', () => {
  it('streams the full object with a 200 when no range is requested', async () => {
    const storageRepository = mockStorageRepository();
    storageRepository.isManagedPath.mockReturnValue(true);
    storageRepository.stat.mockResolvedValue({ size: 11 } as never);
    storageRepository.createReadStream.mockResolvedValue({
      stream: Readable.from([Buffer.from('hello world')]),
      length: 11,
    });

    const { res, body } = makeRes();
    const req = { headers: {} } as never;
    const next = vi.fn();
    const file = new ImmichFileResponse({
      path: '/data/upload/a.jpg',
      contentType: 'image/jpeg',
      cacheControl: CacheControl.None,
    });

    await sendFile(
      res as never,
      req,
      next,
      () => file,
      storageRepository,
      automock(LoggingRepository, { strict: false }),
    );

    expect(res.statusCode).toBeUndefined();
    expect(res.headers['Content-Length']).toBe('11');
    expect(res.headers['Accept-Ranges']).toBe('bytes');
    expect(body()).toBe('hello world');
    expect(next).not.toHaveBeenCalled();
  });

  it('streams a 206 partial response for a valid Range header', async () => {
    const storageRepository = mockStorageRepository();
    storageRepository.isManagedPath.mockReturnValue(true);
    storageRepository.stat.mockResolvedValue({ size: 11 } as never);
    storageRepository.createReadStream.mockImplementation((_path, _type, range) => {
      expect(range).toEqual({ start: 0, end: 4 });
      return Promise.resolve({
        stream: Readable.from([Buffer.from('hello')]),
        length: 5,
      });
    });

    const { res, body } = makeRes();
    const req = { headers: { range: 'bytes=0-4' } } as never;
    const next = vi.fn();
    const file = new ImmichFileResponse({
      path: '/data/upload/a.jpg',
      contentType: 'video/mp4',
      cacheControl: CacheControl.None,
    });

    await sendFile(
      res as never,
      req,
      next,
      () => file,
      storageRepository,
      automock(LoggingRepository, { strict: false }),
    );

    expect(res.statusCode).toBe(206);
    expect(res.headers['Content-Range']).toBe('bytes 0-4/11');
    expect(res.headers['Content-Length']).toBe('5');
    expect(body()).toBe('hello');
  });

  it('falls back to the full object for an unsatisfiable range', async () => {
    const storageRepository = mockStorageRepository();
    storageRepository.isManagedPath.mockReturnValue(true);
    storageRepository.stat.mockResolvedValue({ size: 11 } as never);
    storageRepository.createReadStream.mockImplementation((_path, _type, range) => {
      expect(range).toBeUndefined();
      return Promise.resolve({
        stream: Readable.from([Buffer.from('hello world')]),
        length: 11,
      });
    });

    const { res } = makeRes();
    const req = { headers: { range: 'bytes=99-200' } } as never;
    const next = vi.fn();
    const file = new ImmichFileResponse({
      path: '/data/upload/a.jpg',
      contentType: 'video/mp4',
      cacheControl: CacheControl.None,
    });

    await sendFile(
      res as never,
      req,
      next,
      () => file,
      storageRepository,
      automock(LoggingRepository, { strict: false }),
    );

    expect(res.statusCode).toBeUndefined();
  });
});
