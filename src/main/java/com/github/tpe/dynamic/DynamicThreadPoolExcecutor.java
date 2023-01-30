package com.github.tpe.dynamic;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Thread pool with a dynamic number of threads, but with the maximum size (as
 * opposed to returned by {@link java.util.concurrent.Executors#newCachedThreadPool}).
 * Based on
 * <a href="https://stackoverflow.com/questions/19528304/how-to-get-the-threadpoolexecutor-to-increase-threads-to-max-before-queueing/24493856#24493856">
 * this StackOverflow discussion</a>.
 *
 * @author Piotr Melkowski
 */
public class DynamicThreadPoolExcecutor extends ThreadPoolExecutor {

	/**
	 * Creates a new {@code DynamicThreadPoolExcecutor} with the given initial parameters,
	 * and the {@linkplain Executors#defaultThreadFactory default thread factory}.
	 *
	 * @param maximumPoolSize the maximum number of threads to allow in the pool
	 * @param keepAliveTime the maximum time that excess idle threads will wait for new tasks before terminating
	 * @param unit the time unit for the {@code keepAliveTime} argument
	 *
	 * @throws IllegalArgumentException if one of the following holds:<br>
	 *         {@code maximumPoolSize <= 0}<br>
	 *         {@code keepAliveTime < 0}<br>
	 */
	public DynamicThreadPoolExcecutor(int maximumPoolSize, long keepAliveTime, TimeUnit unit) {
		this(maximumPoolSize, keepAliveTime, unit, Executors.defaultThreadFactory());
	}

	/**
	 * Creates a new {@code DynamicThreadPoolExcecutor} with the given initial parameters.
	 *
	 * @param maximumPoolSize the maximum number of threads to allow in the pool
	 * @param keepAliveTime the maximum time that excess idle threads will wait for new tasks before terminating
	 * @param unit the time unit for the {@code keepAliveTime} argument
	 * @param threadFactory the factory to use when the executor creates a new thread
	 *
	 * @throws IllegalArgumentException if one of the following holds:<br>
	 *         {@code maximumPoolSize <= 0}<br>
	 *         {@code keepAliveTime < 0}<br>
	 * @throws NullPointerException if {@code threadFactory} is null
	 */
	public DynamicThreadPoolExcecutor(int maximumPoolSize, long keepAliveTime, TimeUnit unit,
			ThreadFactory threadFactory) {
		super(0, maximumPoolSize,
				keepAliveTime,
				unit,
				createWorkQueue(),
				threadFactory,
				createRejectedExecutionHandler());
	}

	/**
	 * Creates the queue to use for holding tasks before they are executed.
	 * @return {@link java.util.concurrent.LinkedTransferQueue}) instance that immediately transfers
	 * the offered tasks
	 */
	@SuppressWarnings("serial")
	protected static BlockingQueue<Runnable> createWorkQueue() {
		return new LinkedTransferQueue<Runnable>() {
			@Override
			public boolean offer(Runnable e) {
				/*
				 * When there is a waiting consumer thread the task will just get passed to that
				 * thread. Otherwise, offer() will return false and the ThreadPoolExecutor will
				 * spawn a new thread.
				 */
				return tryTransfer(e);
			}
		};
	}

	/**
	 * Creates the handler to use when execution is blocked because the thread bounds is reached.
	 * @return {@link java.util.concurrent.RejectedExecutionHandler}) implementation that queues
	 * the rejected task again.
	 */
	protected static RejectedExecutionHandler createRejectedExecutionHandler() {
		return new RejectedExecutionHandler() {
			@Override
			public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
				try {
					/*
					 * Queue the task again on rejection.
					 */
					executor.getQueue().put(r);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
		};
	}

}