package info.kgeorgiy.ja.nesterenko.crawler;

import info.kgeorgiy.java.advanced.crawler.*;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.*;
import java.util.concurrent.*;

public class WebCrawler implements AdvancedCrawler {
    private final Downloader downloader;
    private final ExecutorService downloadersService;
    private final ExecutorService extractorsService;
    private final int perHost;
    private final Map<String, DownloadQueue> urlHostMap;

    private class DownloadQueue {
        private final Queue<Runnable> tasks = new ArrayDeque<>();

        private int serviceSize = 0;
        private final int maxSize;

        public DownloadQueue(int maxSize) {
            this.maxSize = maxSize;
        }

        public synchronized void submit() {
            Runnable task = tasks.poll();
            if (task != null) {
                downloadersService.submit(task);
            } else {
                serviceSize--;
            }
        }

        public synchronized void add(Runnable task) {
            if (serviceSize < maxSize) {
                serviceSize++;
                downloadersService.submit(task);
            } else {
                tasks.add(task);
            }
        }

    }

    private static class UrlAndDepth {

        private final String url;
        private final int depth;
        private UrlAndDepth(String url, int depth) {
            this.url = url;
            this.depth = depth;
        }

        public int getDepth() {
            return depth;
        }

        public String getUrl() {
            return url;
        }

    }
    public WebCrawler(Downloader downloader, int downloaders, int extractors, int perHost) {
        this.downloader = downloader;
        downloadersService = Executors.newFixedThreadPool(downloaders);
        extractorsService = Executors.newFixedThreadPool(extractors);
        this.perHost = perHost;
        urlHostMap = new ConcurrentHashMap<>();
    }

    private void breadthFirstSearchDownload(String url,
                                            int depth,
                                            Set<String> downloaded,
                                            Set<String> extracted,
                                            Map<String, IOException> errors,
                                            Phaser phaser) {
        Queue<UrlAndDepth> urls = new ConcurrentLinkedQueue<>();
        urls.add(new UrlAndDepth(url, 1));
        int currentDepth = 0;
        while (!urls.isEmpty()) {
            if (urls.peek().getDepth() > currentDepth) {
                currentDepth++;
                phaser.arriveAndAwaitAdvance();
            }
            UrlAndDepth currentUrl = urls.poll();
            try {
                String hostName = URLUtils.getHost(Objects.requireNonNull(currentUrl).getUrl());
                DownloadQueue downloadQueue = urlHostMap.computeIfAbsent(hostName, name -> new DownloadQueue(perHost));
                phaser.register();
                downloadQueue.add(() -> {
                    try {
                        Document document = downloader.download(currentUrl.getUrl());
                        downloaded.add(currentUrl.getUrl());
                        if (currentUrl.getDepth() < depth) {
                            phaser.register();
                            extractorsService.submit(() -> {
                                try {
                                    document.extractLinks().stream()
                                            .filter(extracted::add)
                                            .forEach(urlToAdd ->
                                                    urls.add(new UrlAndDepth(urlToAdd,
                                                            currentUrl.getDepth() + 1)));
                                } catch (IOException ignored) {
                                } finally {
                                    phaser.arriveAndDeregister();
                                }
                            });
                        }
                    } catch (IOException e) {
                        errors.put(currentUrl.getUrl(), e);
                    } finally {
                        phaser.arriveAndDeregister();
                        downloadQueue.submit();
                    }
                });
            } catch (MalformedURLException e) {
                errors.put(Objects.requireNonNull(currentUrl).getUrl(), e);
            }
            if (urls.isEmpty()) {
                phaser.arriveAndAwaitAdvance();
            }
        }
    }

    @Override
    public Result download(String url, int depth) {
        return download(url, depth, null);
    }

    @Override
    public Result download(String url, int depth, List<String> hosts) {
        Set<String> downloaded = ConcurrentHashMap.newKeySet();
        Set<String> extracted = ConcurrentHashMap.newKeySet();
        extracted.add(url);
        Map<String, IOException> errors = new ConcurrentHashMap<>();
        Phaser phaser = new Phaser(1);
        breadthFirstSearchDownload(url, depth, downloaded, extracted, errors, phaser);
        phaser.arriveAndAwaitAdvance();
        return new Result(new ArrayList<>(downloaded), errors);
    }

    @Override
    public void close() {
        downloadersService.shutdown();
        extractorsService.shutdown();
    }

    private static void checkArgs(String[] args) throws IllegalArgumentException {
        if (args == null || args.length < 1 || args[0] == null) {
            throw new IllegalArgumentException(("Usage: WebCrawler url [depth [downloads [extractors [perHost]]]]"));
        }
    }

    private static String[] formatArgs(String[] args) {
        args = Arrays.copyOf(args, 4);
        for (int i = 0; i < 4; i++) {
            if (args[i] == null) {
                args[i] = "16";
            }
        }
        return args;
    }

    public static void main(String[] args) {
        WebCrawler webCrawler = null;
        try {
            checkArgs(args);
            args = formatArgs(args);
            webCrawler = new WebCrawler(new CachingDownloader(),
                    Integer.parseInt(args[1]),
                    Integer.parseInt(args[2]),
                    Integer.parseInt(args[3]));
            webCrawler.download(args[0], Integer.parseInt(args[1]));
        } catch (NumberFormatException e) {
            System.err.println("Illegal argument format: " + e.getMessage());
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
        } catch (IOException e) {
            System.err.println("Couldn't create CachingDownloader: " + e.getMessage());
        } finally {
            if (webCrawler != null) {
                webCrawler.close();
            }
        }
    }
}
