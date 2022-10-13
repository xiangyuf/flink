/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.ReadableConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.util.FlinkUserCodeClassLoader.NOOP_EXCEPTION_HANDLER;

/** Gives the URLClassLoader a nicer name for debugging purposes. */
@Internal
public class FlinkUserCodeClassLoaders {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkUserCodeClassLoaders.class);
    // the pattern used to parse %ENV_VAR% in class load URLs.
    private static final Pattern parseEnvPattern = Pattern.compile("%[a-zA-Z]\\w*%");

    private FlinkUserCodeClassLoaders() {}

    public static MutableURLClassLoader parentFirst(
            URL[] urls,
            ClassLoader parent,
            Consumer<Throwable> classLoadingExceptionHandler,
            boolean checkClassLoaderLeak) {
        FlinkUserCodeClassLoader classLoader =
                new ParentFirstClassLoader(urls, parent, classLoadingExceptionHandler);
        return wrapWithSafetyNet(classLoader, checkClassLoaderLeak);
    }

    public static MutableURLClassLoader childFirst(
            URL[] urls,
            ClassLoader parent,
            String[] alwaysParentFirstPatterns,
            Consumer<Throwable> classLoadingExceptionHandler,
            boolean checkClassLoaderLeak) {
        FlinkUserCodeClassLoader classLoader =
                new ChildFirstClassLoader(
                        urls, parent, alwaysParentFirstPatterns, classLoadingExceptionHandler);
        return wrapWithSafetyNet(classLoader, checkClassLoaderLeak);
    }

    public static MutableURLClassLoader create(
            final URL[] urls, final ClassLoader parent, final ReadableConfig config) {
        final String[] alwaysParentFirstLoaderPatterns =
                CoreOptions.getParentFirstLoaderPatterns(config);
        final String classLoaderResolveOrder = config.get(CoreOptions.CLASSLOADER_RESOLVE_ORDER);
        final FlinkUserCodeClassLoaders.ResolveOrder resolveOrder =
                FlinkUserCodeClassLoaders.ResolveOrder.fromString(classLoaderResolveOrder);
        final boolean checkClassloaderLeak = config.get(CoreOptions.CHECK_LEAKED_CLASSLOADER);
        return create(
                resolveOrder,
                urls,
                parent,
                alwaysParentFirstLoaderPatterns,
                NOOP_EXCEPTION_HANDLER,
                checkClassloaderLeak);
    }

    public static MutableURLClassLoader create(
            ResolveOrder resolveOrder,
            URL[] urls,
            ClassLoader parent,
            String[] alwaysParentFirstPatterns,
            Consumer<Throwable> classLoadingExceptionHandler,
            boolean checkClassLoaderLeak) {
        URL[] finalUrls = replaceEnvVarInUrlIfRequired(urls);
        LOG.info(
                "created classloader with url: {} and resolveOrder: {}",
                Arrays.toString(finalUrls),
                resolveOrder);
        switch (resolveOrder) {
            case CHILD_FIRST:
                return childFirst(
                        finalUrls,
                        parent,
                        alwaysParentFirstPatterns,
                        classLoadingExceptionHandler,
                        checkClassLoaderLeak);
            case PARENT_FIRST:
                return parentFirst(
                        finalUrls, parent, classLoadingExceptionHandler, checkClassLoaderLeak);
            default:
                throw new IllegalArgumentException(
                        "Unknown class resolution order: " + resolveOrder);
        }
    }

    private static MutableURLClassLoader wrapWithSafetyNet(
            FlinkUserCodeClassLoader classLoader, boolean check) {
        return check
                ? new SafetyNetWrapperClassLoader(classLoader, classLoader.getParent())
                : classLoader;
    }

    /**
     * Replace the %ENV_VAR% to the actual env var value in URLs.
     *
     * @param urls the urls waiting to parse and replace
     * @return the env var replaced urls
     */
    private static URL[] replaceEnvVarInUrlIfRequired(URL[] urls) {
        URL[] finalUrls = new URL[urls.length];
        for (int i = 0; i < urls.length; i++) {
            String replacedUrl = urls[i].toString();
            Matcher matcher = parseEnvPattern.matcher(replacedUrl);
            while (matcher.find()) {
                String matchedStr = matcher.group();
                String envVar = System.getenv(matchedStr.substring(1, matchedStr.length() - 1));
                if (StringUtils.isNullOrWhitespaceOnly(envVar)) {
                    LOG.error("can not find given env var {} in this url {}", matchedStr, urls[i]);
                    continue;
                }
                replacedUrl = matcher.replaceFirst(envVar);
                matcher = parseEnvPattern.matcher(replacedUrl);
            }
            try {
                finalUrls[i] = new URL(replacedUrl);
            } catch (MalformedURLException e) {
                // should never be reached
                throw new RuntimeException(e);
            }
        }
        return finalUrls;
    }

    /** Class resolution order for Flink URL {@link ClassLoader}. */
    public enum ResolveOrder {
        CHILD_FIRST,
        PARENT_FIRST;

        public static ResolveOrder fromString(String resolveOrder) {
            if (resolveOrder.equalsIgnoreCase("parent-first")) {
                return PARENT_FIRST;
            } else if (resolveOrder.equalsIgnoreCase("child-first")) {
                return CHILD_FIRST;
            } else {
                throw new IllegalArgumentException("Unknown resolve order: " + resolveOrder);
            }
        }
    }

    /**
     * Regular URLClassLoader that first loads from the parent and only after that from the URLs.
     */
    @Internal
    public static class ParentFirstClassLoader extends FlinkUserCodeClassLoader {

        ParentFirstClassLoader(
                URL[] urls, ClassLoader parent, Consumer<Throwable> classLoadingExceptionHandler) {
            super(urls, parent, classLoadingExceptionHandler);
        }

        static {
            ClassLoader.registerAsParallelCapable();
        }
    }

    /**
     * Ensures that holding a reference on the context class loader outliving the scope of user code
     * does not prevent the user classloader to be garbage collected (FLINK-16245).
     *
     * <p>This classloader delegates to the actual user classloader. Upon {@link #close()}, the
     * delegate is nulled and can be garbage collected. Additional class resolution will be resolved
     * solely through the bootstrap classloader and most likely result in ClassNotFound exceptions.
     */
    @Internal
    public static class SafetyNetWrapperClassLoader extends MutableURLClassLoader {
        private static final Logger LOG =
                LoggerFactory.getLogger(SafetyNetWrapperClassLoader.class);

        protected volatile FlinkUserCodeClassLoader inner;

        protected SafetyNetWrapperClassLoader(FlinkUserCodeClassLoader inner, ClassLoader parent) {
            super(new URL[0], parent);
            this.inner = inner;
        }

        @Override
        public void close() {
            final FlinkUserCodeClassLoader inner = this.inner;
            if (inner != null) {
                try {
                    inner.close();
                } catch (IOException e) {
                    LOG.warn("Could not close user classloader", e);
                }
            }
            this.inner = null;
        }

        private FlinkUserCodeClassLoader ensureInner() {
            if (inner == null) {
                throw new IllegalStateException(
                        "Trying to access closed classloader. Please check if you store "
                                + "classloaders directly or indirectly in static fields. If the stacktrace suggests that the leak "
                                + "occurs in a third party library and cannot be fixed immediately, you can disable this check "
                                + "with the configuration '"
                                + CoreOptions.CHECK_LEAKED_CLASSLOADER.key()
                                + "'.");
            }
            return inner;
        }

        @Override
        public Class<?> loadClass(String name) throws ClassNotFoundException {
            return ensureInner().loadClass(name);
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            // called for dynamic class loading
            return ensureInner().loadClass(name, resolve);
        }

        @Override
        public void addURL(URL url) {
            ensureInner().addURL(url);
        }

        @Override
        public URL getResource(String name) {
            return ensureInner().getResource(name);
        }

        @Override
        public Enumeration<URL> getResources(String name) throws IOException {
            return ensureInner().getResources(name);
        }

        @Override
        public URL[] getURLs() {
            return ensureInner().getURLs();
        }

        static {
            ClassLoader.registerAsParallelCapable();
        }
    }
}
