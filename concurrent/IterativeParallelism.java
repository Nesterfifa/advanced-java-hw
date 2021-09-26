package info.kgeorgiy.ja.nesterenko.concurrent;

import info.kgeorgiy.java.advanced.concurrent.AdvancedIP;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class IterativeParallelism implements AdvancedIP {
    private <T, R> R concurrentReducer(final int threads,
                                       final List<? extends T> values,
                                       final Function<? super Stream<? extends T>, R> applyFunction,
                                       final Function<? super Stream<R>, R> reduceFunction) throws InterruptedException {
        return reduceFunction.apply(concurrentApplier(applyFunction, split(threads, values)).stream());
    }

    private <T, R> List<R> concurrentApplier(final Function<? super Stream<? extends T>, R> applyFunction,
                                             final List<Stream<? extends T>> parts) throws InterruptedException {
        final int partsCnt = parts.size();
        final List<Thread> threads = new ArrayList<>();
        final List<R> res = new ArrayList<>(Collections.nCopies(partsCnt, null));
        for (int i = 0; i < partsCnt; i++) {
            final int index = i;
            Thread thread = new Thread(() -> res.set(index, applyFunction.apply(parts.get(index))));
            threads.add(thread);
            thread.start();
        }
        joinThreads(threads);
        return res;
    }

    private static <T> List<Stream<? extends T>> split(int threads, final List<? extends T> values) {
        final int len = values.size();
        threads = Math.min(len, threads);
        final int blockSize = len / threads;
        int rem = len % threads;
        final List<Stream<? extends T>> res = new ArrayList<>();
        int l = 0;
        for (int i = 0; i < threads; i++) {
            final int r = l + blockSize + (rem-- > 0 ? 1 : 0);
            res.add(values.subList(l, r).stream());
            l = r;
        }
        return res;
    }

    private static void joinThreads(List<Thread> threads) throws InterruptedException {
        for (int i = 0; i < threads.size(); i++) {
            try {
                threads.get(i).join();
            } catch (InterruptedException e) {
                final InterruptedException exception = new InterruptedException("Failed to join threads");
                for (int j = i; j < threads.size(); j++) {
                    threads.get(j).interrupt();
                }
                for (int j = i; j < threads.size(); j++) {
                    while (true) {
                        try {
                            threads.get(j).join();
                            break;
                        } catch (InterruptedException ignored) {

                        }
                    }
                }
                throw exception;
            }
        }
    }

    @Override
    public <T> T reduce(int threads, List<T> values, Monoid<T> monoid) throws InterruptedException {
        return mapReduce(threads, values, Function.identity(), monoid);
    }

    @Override
    public <T, R> R mapReduce(int threads, List<T> values, Function<T, R> lift, Monoid<R> monoid) throws InterruptedException {
        final Function<? super Stream<R>, R> monoidReducer =
                stream -> stream.reduce(monoid.getOperator()).orElse(monoid.getIdentity());
        return concurrentReducer(threads, values,
                stream -> monoidReducer.apply(stream.map(lift)),
                monoidReducer);
    }

    @Override
    public String join(int threads, List<?> values) throws InterruptedException {
        return concurrentReducer(threads,
                values,
                stream -> stream.map(Object::toString).collect(Collectors.joining()),
                stream -> stream.collect(Collectors.joining()));
    }

    private <T, R> List<R> appliedList(int threads,
                                       final List<? extends T> values,
                                       final Function<Stream<? extends T>, Stream<? extends R>> streamFunction) throws InterruptedException {
        return concurrentReducer(threads, values,
                stream -> streamFunction.apply(stream).collect(Collectors.toList()),
                stream -> stream.flatMap(Collection::stream).collect(Collectors.toList()));
    }

    @Override
    public <T> List<T> filter(int threads, List<? extends T> values, Predicate<? super T> predicate) throws InterruptedException {
        return appliedList(threads, values, stream -> stream.filter(predicate));
    }

    @Override
    public <T, U> List<U> map(int threads, List<? extends T> values, Function<? super T, ? extends U> f) throws InterruptedException {
        return appliedList(threads, values, stream -> stream.map(f));
    }

    @Override
    public <T> T maximum(int threads, List<? extends T> values, Comparator<? super T> comparator) throws InterruptedException {
        final Function<Stream<? extends T>, T> streamMax = stream -> stream.max(comparator).orElse(null);
        return concurrentReducer(threads, values, streamMax, streamMax);
    }

    @Override
    public <T> T minimum(int threads, List<? extends T> values, Comparator<? super T> comparator) throws InterruptedException {
        return maximum(threads, values, comparator.reversed());
    }

    @Override
    public <T> boolean all(int threads, List<? extends T> values, Predicate<? super T> predicate) throws InterruptedException {
        return !any(threads, values, predicate.negate());
    }

    @Override
    public <T> boolean any(int threads, List<? extends T> values, Predicate<? super T> predicate) throws InterruptedException {
        return !filter(threads, values, predicate).isEmpty();
    }
}
