package completablefuture.test;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.concurrent.ConcurrentUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

@Slf4j
public class ComplitableFutureSampleTest {

    @Test
    public void testAction() {
        val urls = Arrays.asList("www.google.com", "www.google2.com", "g.com");
        Assert.assertEquals(2, action(urls));
    }

    private int action(List<String> urls) {
      List<CompletableFuture<Pair<String, String>>> futures = urls.stream().map(this::getUrlAndStatusPair).collect(toList());
      List<Pair<String, String>> urlAndStatusPairs = futures.stream().map(CompletableFuture::join).collect(toList());
      List<CompletableFuture<String>> transformUrlStream = urlAndStatusPairs.stream()
          .filter(this::isUrlActive)
          .map(this::transformUrl)
          .map(fut -> fut.exceptionally(this::onErrorHandler))
          .collect(toList());
      return transformUrlStream.stream().map(CompletableFuture::join).reduce(0, this::countUrls, (a,b) -> (a+b));
    }

    private CompletableFuture<String> transformUrl(Pair<String, String> pair) {
        //By Default it uses thread pool with number of CPU what is needed for CPU bound operations.
        return CompletableFuture.supplyAsync(() -> this.transformUrlOperation(pair.getLeft()));
    }

    private CompletableFuture<Pair<String, String>> getUrlAndStatusPair(String url) {
        return callHystrixCommand(url).thenApply(status -> Pair.of(url, status));
    }

    private CompletableFuture<String> callHystrixCommand(String url) {
        //emulate hystrix command with queue() method which  returns Future
        return CompletableFuture.supplyAsync(() -> "active", ioExecutor());
    }

  private Executor ioExecutor() {
    Executor executor = Executors.newFixedThreadPool( 100, new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        Thread thread = new Thread(r);
        thread.setDaemon(true);
        return thread;
      }
    });
    return executor;
  }

  private String transformUrlOperation(final String url) {
        //emulates high intensive CPU operation
        return url.toLowerCase();
    }

    private boolean isUrlActive(Pair<String, String> pair) {
        return "active".equalsIgnoreCase(pair.getRight());
    }

    private int countUrls(int counter, String url) {
        if (url.length() > 5) {
            return counter + 1;
        } else {
            return counter;
        }
    }

    private String onErrorHandler(Throwable err) {
        log.error("Can not make some action", err);
    }
}