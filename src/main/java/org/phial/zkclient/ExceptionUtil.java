
package org.phial.zkclient;

import io.netty.util.internal.StringUtil;
import org.apache.zookeeper.common.StringUtils;
import org.phial.zkclient.exception.ZkInterruptedException;

import java.util.function.Consumer;

public class ExceptionUtil {

    public static RuntimeException convertToRuntimeException(Throwable e) {
        if (e instanceof RuntimeException) {
            return (RuntimeException) e;
        }
        retainInterruptFlag(e);
        return new RuntimeException(e);
    }

    /**
     * This sets the interrupt flag if the catched exception was an {@link InterruptedException}. Catching such an
     * exception always clears the interrupt flag.
     *
     * @param catchedException The catched exception.
     */
    public static void retainInterruptFlag(Throwable catchedException) {
        if (catchedException instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
    }

    public static void rethrowInterruptedException(Throwable e) throws InterruptedException {
        if (e instanceof InterruptedException) {
            throw (InterruptedException) e;
        }
        if (e instanceof ZkInterruptedException) {
            throw (ZkInterruptedException) e;
        }
    }

    ///////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////

    @FunctionalInterface
    public interface ThrowExceptionFunction {
        /**
         * exception msg
         *
         * @param message
         */
        void throwMessage(String message);
    }

    @FunctionalInterface
    public interface BranchHandle {
        /**
         * @param trueHandle
         * @param falseHandle
         */
        void trueOrFalseHandle(Runnable trueHandle, Runnable falseHandle);
    }

    public interface PresentOrElseHandler<T extends Object> {
        /**
         * 值不为空时执行消费操作
         * 值为空时执行其他的操作
         *
         * @param action      值不为空时，执行的消费操作
         * @param emptyAction 值为空时，执行的操作
         * @return void
         **/
        void presentOrElseHandle(Consumer<? super T> action, Runnable emptyAction);
    }

    ///////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////

    public static BranchHandle isTureOrFalse(boolean b) {
        return (trueHandle, falseHandle) -> {
            Runnable handler = b ? trueHandle : falseHandle;
            handler.run();
        };
    }

    public static ThrowExceptionFunction isTure(boolean b) {
        return (errorMessage) -> {
            if (b) {
                throw new RuntimeException(errorMessage);
            }
        };
    }

    public static PresentOrElseHandler<?> isBlankOrNoBlank(String str) {
        return (consumer, runnable) -> {
            if (StringUtil.isNullOrEmpty(str)) {
                runnable.run();
            } else {
                consumer.accept(str);
            }
        };
    }

}
