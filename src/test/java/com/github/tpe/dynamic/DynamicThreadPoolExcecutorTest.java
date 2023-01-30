package com.github.tpe.dynamic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class DynamicThreadPoolExcecutorTest {

	private static final int MAX_POOL_SIZE = 13;

	private static DynamicThreadPoolExcecutor executor;

	@BeforeAll
	public static void setup() {
		executor = new DynamicThreadPoolExcecutor(MAX_POOL_SIZE, 1, TimeUnit.SECONDS);
	}

	@AfterAll
	public static void shutdown() {
		executor.shutdownNow();
	}

	@ParameterizedTest
	@MethodSource("getTaskCounts")
	public void testExecution(int taskCount) throws Exception {
		List<Future<String>> tasks = submitTasks(new ExecutorCompletionService<>(executor), taskCount);
		assertEquals(taskCount, getResults(tasks).count());
	}

	@ParameterizedTest
	@MethodSource("getTaskCounts")
	public void testDistribution(int taskCount) throws Exception {
		List<Future<String>> tasks = submitTasks(new ExecutorCompletionService<>(executor), taskCount);
		// thread name -> number of executions
		Map<String, Long> threadMap = getResults(tasks)
			.collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

		int threadsUsed = threadMap.size();
		assertEquals(Math.min(taskCount, MAX_POOL_SIZE), threadsUsed);

		long minExecutions = threadMap.values().stream()
			.mapToLong(Long::longValue)
			.min()
			.getAsLong();
		long maxExecutions = threadMap.values().stream()
			.mapToLong(Long::longValue)
			.max()
			.getAsLong();
		assertTrue(maxExecutions - minExecutions <= 1);
	}

	@ParameterizedTest
	@MethodSource("getTaskCounts")
	public void testSharing(int taskCount) throws Exception {
		CompletionService<String> service1 = new ExecutorCompletionService<>(executor);
		CompletionService<String> service2 = new ExecutorCompletionService<>(executor);

		List<Future<String>> tasks1 = new LinkedList<>();
		List<Future<String>> tasks2 = new LinkedList<>();
		IntStream.rangeClosed(1, taskCount).forEach(i -> {
			tasks1.add(service1.submit(newTask()));
			tasks2.add(service2.submit(newTask()));
		});

		assertEquals(taskCount, getResults(tasks1).count());
		assertEquals(taskCount, getResults(tasks2).count());
	}

	@ParameterizedTest
	@MethodSource("getTaskCounts")
	public void testTeardown(int taskCount) throws Exception {
		assertEquals(0, executor.getActiveCount());
		getResults(submitTasks(new ExecutorCompletionService<>(executor), taskCount));
		Thread.sleep(1000 + taskCount / 10);
		assertEquals(0, executor.getActiveCount());
	}

	private static IntStream getTaskCounts() {
		return IntStream.of(1,
				MAX_POOL_SIZE - 1, MAX_POOL_SIZE, MAX_POOL_SIZE + 1,
				2 * MAX_POOL_SIZE, 2 * MAX_POOL_SIZE + 1,
				MAX_POOL_SIZE * 100 + MAX_POOL_SIZE / 2 + 1);
	}

	private static List<Future<String>> submitTasks(CompletionService<String> service, int count) {
		return IntStream.rangeClosed(1, count)
			.mapToObj(i -> newTask())
			.map(service::submit)
			.collect(Collectors.toList());
	}

	private static Callable<String> newTask() {
		return new Callable<String>() {
			@Override
			public String call() throws Exception {
				Thread.sleep(10);
				return Thread.currentThread().getName();
			}
		};
	}

	private static Stream<String> getResults(List<Future<String>> tasks) {
		return tasks.stream().map(t -> {
			try {
				return t.get();
			} catch (InterruptedException | ExecutionException e) {
				throw new RuntimeException(e);
			}
		});
	}

}
