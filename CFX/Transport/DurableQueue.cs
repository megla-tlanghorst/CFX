using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CFX.Utilities;

namespace CFX.Transport
{
    internal class DurableQueue
    {
        public DurableQueue(string name)
            : base()
        {
            dataName = Path.GetTempPath();
            string safeName = name.Replace('\\', '-');
            safeName = safeName.Replace('/', '-');
            dataName += (@"\" + safeName + ".cache");
            Initialize();
        }

        ~DurableQueue()
        {
            Close();
        }

        private string dataName = "";
        private BinaryWriter dataWriter = null;
        private BinaryReader dataReader = null;

        private uint fileSignature
        {
            get { return 0xfe982422; }
        }

        private uint fileVersion
        {
            get { return pvtFileVersion; }
            set { pvtFileVersion = value; }
        }

        private uint pvtFileVersion = 1;
        private SemaphoreSlim syncSemaphore = new SemaphoreSlim(1, 1);
        private List<CFXEnvelope> queue = new List<CFXEnvelope>();
        private ConcurrentQueue<Action<CFXEnvelope>> listeners = new ConcurrentQueue<Action<CFXEnvelope>>();

        private void Initialize()
        {
            // Create and/open cache file and load existing cache into memory
            try
            {
                // Clear in-memory queue
                queue.Clear();

                // Create and Open Cache Index and Data files
                FileStream dataFile = new FileStream(dataName, FileMode.OpenOrCreate, FileAccess.ReadWrite,
                    FileShare.ReadWrite);
                dataReader = new BinaryReader(dataFile, System.Text.Encoding.UTF8);
                dataWriter = new BinaryWriter(dataFile, System.Text.Encoding.UTF8);
                ReadData();
                OpenWriter();
            }
            catch (Exception e)
            {
                AppLog.Error(e);
            }
        }

        public bool IsEmpty
        {
            get
            {
                syncSemaphore.Wait();
                try
                {
                    return queue.Count < 1;
                }
                finally
                {
                    syncSemaphore.Release();
                }
            }
        }

        public int Count => queue.Count;

        private bool ReadData()
        {
            bool result = false;

            syncSemaphore.Wait();
            try
            {
                try
                {
                    queue.Clear();
                    if (dataReader == null) return false;
                    if (dataReader.BaseStream.Length < 1) return true;
                    dataReader.BaseStream.Seek(0, SeekOrigin.Begin);

                    // Read the File Signature
                    uint sig = dataReader.ReadUInt32();
                    if (sig == this.fileSignature)
                    {
                        // Read the Version Tag
                        fileVersion = dataReader.ReadUInt32();

                        // Read the Cache Size
                        int cacheSize = dataReader.ReadInt32();

                        for (int i = 0; i < cacheSize; i++)
                        {
                            CFXEnvelope rec = CFXEnvelope.ReadRecord(dataReader);
                            if (rec != null && !rec.Transmitted)
                            {
                                if (!rec.Transmitted) queue.Add(rec);
                            }
                        }

                        result = true;
                    }
                }
                catch (Exception e)
                {
                    AppLog.Error(e);
                }
            }
            finally
            {
                syncSemaphore.Release();
            }

            if (!result) queue.Clear();
            return result;
        }

        private bool OpenWriter()
        {
            try
            {
                if (dataWriter == null) return false;

                // Validate File
                if (dataWriter.BaseStream.Length > 0)
                {
                    dataWriter.BaseStream.Seek(0, SeekOrigin.End);
                }
                else
                {
                    // Write Signature
                    dataWriter.Write(fileSignature);

                    // Write File Version
                    dataWriter.Write(fileVersion);

                    // Write the Cache Size
                    dataWriter.Write(0);

                    dataWriter.Flush();
                }

                return true;
            }
            catch (Exception e)
            {
                AppLog.Error(e);
            }

            return false;
        }

        public void Close()
        {
            dataWriter?.Dispose();
            dataReader?.Dispose();
            dataWriter = null;
            dataReader = null;
        }

        public bool Enqueue(CFXEnvelope obj)
        {
            bool result = false;

            if (TryCallListener(obj))
            {
                return true;
            }
            
            syncSemaphore.Wait();
            try
            {
                
                try
                {
                    obj.Transmitted = false;
                    if (dataWriter != null)
                    {
                        obj.WriteRecord(dataWriter);

                        dataWriter.BaseStream.Seek(8, SeekOrigin.Begin);
                        int cacheSize = dataReader.ReadInt32();
                        int curCount = cacheSize + 1;
                        dataWriter.BaseStream.Seek(8, SeekOrigin.Begin);
                        dataWriter.Write(curCount);
                        dataWriter.BaseStream.Seek(0, SeekOrigin.End);
                        dataWriter.Flush();
                    }

                    queue.Add(obj);
                    result = true;
                }
                catch (Exception e)
                {
                    AppLog.Error(e);
                }
            }
            finally
            {
                syncSemaphore.Release();
            }

            return result;
        }

        public CFXEnvelope Peek()
        {
            CFXEnvelope result = null;

            try
            {
                if (queue.Count > 0) result = queue[0];
            }
            catch (Exception e)
            {
                AppLog.Error(e);
            }

            return result;
        }

        public CFXEnvelope[] PeekMany(int maxCount)
        {
            CFXEnvelope[] result = null;

            try
            {
                if (queue.Count > 0)
                {
                    result = queue.Take(queue.Count < maxCount ? queue.Count : maxCount).ToArray();
                }
            }
            catch (Exception e)
            {
                AppLog.Error(e);
            }

            return result;
        }

        public CFXEnvelope[] Dequeue(int count = 1)
        {
            CFXEnvelope[] result = null;

            syncSemaphore.Wait();
            try
            {
                result = PeekMany(count);
                if (result != null)
                {
                    queue.RemoveRange(0, result.Length);
                    if (queue.Count < 1)
                    {
                        InternalClear();
                    }
                    else
                    {
                        foreach (CFXEnvelope env in result) env.SetRecordTransmitted(dataWriter);
                    }
                }
            }
            catch (Exception e)
            {
                AppLog.Error(e);
            }
            finally
            {
                syncSemaphore.Release();
            }

            return result;
        }

        public async Task<CFXEnvelope[]> DequeueAsync(int count = 1)
        {
            CFXEnvelope[] result = null;

            await syncSemaphore.WaitAsync();
            try
            {
                result = PeekMany(count);
                if (result != null)
                {
                    queue.RemoveRange(0, result.Length);
                    if (queue.Count < 1)
                    {
                        InternalClear();
                    }
                    else
                    {
                        foreach (CFXEnvelope env in result) env.SetRecordTransmitted(dataWriter);
                    }
                }
            }
            catch (Exception e)
            {
                AppLog.Error(e);
            }
            finally
            {
                syncSemaphore.Release();
            }

            return result;
        }

        public async Task<CFXEnvelope> DequeueOrWaitAsync(CancellationToken ct = default)
        {
            TaskCompletionSource<CFXEnvelope> tcs = new TaskCompletionSource<CFXEnvelope>();
            await syncSemaphore.WaitAsync(ct);
            try
            {
                if (queue.Count > 0)
                {
                    CFXEnvelope element = queue.First();
                    queue.RemoveAt(0);

                    if (queue.Count <= 0)
                    {
                        InternalClear();
                    }
                    else
                    {
                        element.SetRecordTransmitted(dataWriter);
                    }

                    return element;
                }
                else
                {
                    ct.Register(() => tcs.TrySetCanceled(ct));
                    listeners.Enqueue(tcs.SetResult);
                }
            }
            finally
            {
                syncSemaphore.Release();
            }
            
            return await tcs.Task;
        }

        private bool TryCallListener(CFXEnvelope element)
        {
            if (listeners.TryDequeue(out Action<CFXEnvelope> listener))
            {
                listener.Invoke(element);
                return true;
            }

            return false;
        }

        public void Clear()
        {
            syncSemaphore.Wait();
            try
            {
                InternalClear();
            }
            finally
            {
                syncSemaphore.Release();
            }
        }

        private void InternalClear()
        {
            try
            {
                queue.Clear();
                if (dataWriter != null)
                {
                    dataWriter.BaseStream.SetLength(0);
                    dataWriter.Flush();
                    OpenWriter();
                }
            }
            catch (Exception e)
            {
                AppLog.Error(e);
            }
        }
    }
}