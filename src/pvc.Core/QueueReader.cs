using System;
using System.Threading;
using log4net;

namespace pvc.Core
{
	public class QueueReader<T> : Produces<T> where T : Message
	{
		private readonly IQueue<T> _queue;
		private Consumes<T> _consumer;
		private volatile bool continueRunning;
		private readonly ILog logger = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
		private Thread thread;

		public delegate void LogError(string format, params object[] args);

		public QueueReader(IQueue<T> queue)
		{
			_queue = queue;
		}

		public QueueReader(IQueue<T> queue, ILog logger)
		{
			_queue = queue;
			this.logger = logger;
		}

		private void Run()
		{
			while (continueRunning)
			{
				try
				{
					T item;
					if (_queue.TryDequeue(out item))
					{
						try
						{
							_consumer.Handle(item);
							_queue.MarkComplete(item);
						}
						catch (Exception exception)
						{
							logger.ErrorFormat("Error handling object of type {0}:{1}{2}", object.Equals(item, default(T)) ? "(null)" : item.GetType().Name, Environment.NewLine, exception);
							// requeue?
						}
					}
				}
				catch (Exception exception)
				{
					logger.ErrorFormat("Error dequeuing object of type {0}:{1}{2}", typeof(T).Name, Environment.NewLine, exception);
					//Dead letter
					//Stop?
					//??
				}
			}
		}

		public void Start()
		{
			if (thread != null)
			{
				throw new InvalidOperationException("Start() called while reader already running.");
			}
			thread = new Thread(Run) { IsBackground = true };
			thread.Start();
		}

		public void Stop()
		{
			Thread currentThread = thread;
			thread = null;
			if (currentThread == null) return;
			continueRunning = false;
			currentThread.Join();
		}

		public void AttachConsumer(Consumes<T> consumer)
		{
			_consumer = consumer;
		}
	}
}