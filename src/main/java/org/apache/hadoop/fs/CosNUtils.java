package org.apache.hadoop.fs;

import com.qcloud.cos.auth.COSCredentialsProvider;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.auth.*;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.security.*;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;

import static org.apache.hadoop.fs.cosn.Constants.*;

public final class CosNUtils {
    private static final Logger LOG = LoggerFactory.getLogger(CosNUtils.class);

    static final String INSTANTIATION_EXCEPTION = "instantiation exception";
    static final String NOT_COS_CREDENTIAL_PROVIDER = "is not cos credential provider";
    static final String ABSTRACT_CREDENTIAL_PROVIDER = "is abstract and therefore cannot be created";

    private CosNUtils() {
    }

    public static NativeFileSystemStore createDefaultStore(Configuration conf) {
        NativeFileSystemStore store = new CosNativeFileSystemStore();
        RetryPolicy basePolicy =
                RetryPolicies.retryUpToMaximumCountWithFixedSleep(
                        conf.getInt(CosNConfigKeys.COSN_MAX_RETRIES_KEY,
                                CosNConfigKeys.DEFAULT_MAX_RETRIES),
                        conf.getLong(CosNConfigKeys.COSN_RETRY_INTERVAL_KEY,
                                CosNConfigKeys.DEFAULT_RETRY_INTERVAL),
                        TimeUnit.SECONDS);
        Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap =
                new HashMap<Class<? extends Exception>, RetryPolicy>();

        exceptionToPolicyMap.put(IOException.class, basePolicy);
        RetryPolicy methodPolicy =
                RetryPolicies.retryByException(RetryPolicies.TRY_ONCE_THEN_FAIL,
                        exceptionToPolicyMap);
        Map<String, RetryPolicy> methodNameToPolicyMap = new HashMap<String, RetryPolicy>();

        return (NativeFileSystemStore) RetryProxy.create(NativeFileSystemStore.class, store,
                methodNameToPolicyMap);
    }

    /**
     *  create cos cred
     * @param uri cos uri
     * @param conf config
     * @return provider list
     * @throws IOException
     */
    public static COSCredentialProviderList createCosCredentialsProviderSet(
            URI uri, Configuration conf,
            RangerCredentialsClient rangerClient) throws IOException {
        COSCredentialProviderList credentialProviderList =
                new COSCredentialProviderList();

        Class<?>[] cosClasses = CosNUtils.loadCosProviderClasses(
                conf,
                CosNConfigKeys.COSN_CREDENTIALS_PROVIDER);
        if (0 == cosClasses.length) {
            credentialProviderList.add(new SessionCredentialProvider(uri,
                    conf));
            credentialProviderList.add(new SimpleCredentialProvider(uri, conf));
            credentialProviderList.add(new EnvironmentVariableCredentialProvider(uri, conf));
            credentialProviderList.add(new SessionTokenCredentialProvider(uri,conf));
            credentialProviderList.add(new CVMInstanceCredentialsProvider(uri, conf));
            credentialProviderList.add(new CPMInstanceCredentialsProvider(uri, conf));
            credentialProviderList.add(new EMRInstanceCredentialsProvider(uri, conf));
        } else {
            for (Class<?> credClass : cosClasses) {
                credentialProviderList.add(createCOSCredentialProvider(uri, conf, rangerClient, credClass));
            }
        }

        return credentialProviderList;
    }

    public static Class<?>[] loadCosProviderClasses(
            Configuration conf,
            String key,
            Class<?>... defaultValue) throws IOException {
        try {
            return conf.getClasses(key, defaultValue);
        } catch (RuntimeException e) {
            Throwable c = e.getCause() != null ? e.getCause() : e;
            throw new IOException("From option " + key + ' ' + c, c);
        }
    }

    /**
     * create cos cred
     * @param uri cos uri
     * @param conf config
     * @param credClass cred class
     * @return provider
     * @throws IOException
     */
    public static COSCredentialsProvider createCOSCredentialProvider(
            URI uri, Configuration conf,
            RangerCredentialsClient rangerClient,
            Class<?> credClass) throws IOException {
        COSCredentialsProvider credentialsProvider;
        if (!COSCredentialsProvider.class.isAssignableFrom(credClass)) {
            throw new IllegalArgumentException("class " + credClass + " " + NOT_COS_CREDENTIAL_PROVIDER);
        }
        if (Modifier.isAbstract(credClass.getModifiers())) {
            throw new IllegalArgumentException("class " + credClass + " " + ABSTRACT_CREDENTIAL_PROVIDER);
        }
        LOG.debug("Credential Provider class: " + credClass.getName());

        try {
            // new credClass()
            Constructor constructor = getConstructor(credClass);
            if (constructor != null) {
                credentialsProvider =
                        (COSCredentialsProvider) constructor.newInstance();
                return credentialsProvider;
            }
            // new credClass(conf)
            constructor = getConstructor(credClass, Configuration.class);
            if (null != constructor) {
                credentialsProvider =
                        (COSCredentialsProvider) constructor.newInstance(conf);
                return credentialsProvider;
            }

            // new credClass(uri, conf)
            constructor = getConstructor(credClass, URI.class,
                    Configuration.class);
            if (null != constructor) {
                credentialsProvider =
                        (COSCredentialsProvider) constructor.newInstance(uri,
                                conf);
                return credentialsProvider;
            }

            // new credClass(uri, conf, rangerClient)
            constructor = getConstructor(credClass, URI.class, Configuration.class, RangerCredentialsClient.class);
            if (null != constructor) {
                LOG.info("success get constructor of ranger provider");
                credentialsProvider =
                        (COSCredentialsProvider) constructor.newInstance(uri,
                                conf, rangerClient);
                return credentialsProvider;
            }

            Method factory = getFactoryMethod(credClass,
                    COSCredentialsProvider.class, "getInstance");
            if (null != factory) {
                credentialsProvider =
                        (COSCredentialsProvider) factory.invoke(null);
                return credentialsProvider;
            }

            throw new IllegalArgumentException(
                    "Not supported constructor or factory method found");

        } catch (IllegalAccessException e) {
            throw new IOException(credClass.getName() + " " + INSTANTIATION_EXCEPTION + ": " + e,
                    e);
        } catch (InstantiationException e) {
            throw new IOException(credClass.getName() + " " + INSTANTIATION_EXCEPTION + ": " + e,
                    e);
        } catch (InvocationTargetException e) {
            Throwable targetException = e.getTargetException();
            if (targetException == null) {
                targetException = e;
            }
            throw new IOException(credClass.getName() + " " + INSTANTIATION_EXCEPTION + ": " + targetException,
                    targetException);
        }
    }

    private static Constructor<?> getConstructor(Class<?> cl,
                                                 Class<?>... args) {
        try {
            Constructor constructor = cl.getDeclaredConstructor(args);
            return Modifier.isPublic(constructor.getModifiers()) ? constructor : null;
        } catch (NoSuchMethodException e) {
            return null;
        }
    }

    private static Method getFactoryMethod(Class<?> cl, Class<?> returnType,
                                           String methodName) {
        try {
            Method m = cl.getDeclaredMethod(methodName);
            if (Modifier.isPublic(m.getModifiers())
                    && Modifier.isStatic(m.getModifiers())
                    && returnType.isAssignableFrom(m.getReturnType())) {
                return m;
            } else {
                return null;
            }
        } catch (NoSuchMethodException e) {
            return null;
        }
    }

    public static String getBucketNameWithAppid(String originBucketName, String appidStr) {
        if (appidStr == null || appidStr.isEmpty()) {
            return originBucketName;
        }
        if (originBucketName.endsWith("-"+appidStr)) {
            return originBucketName;
        }
        return originBucketName + "-" + appidStr;
    }


    public static KeyPair loadAsymKeyPair(String pubKeyPath, String priKeyPath)
            throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
        // load public
        File filePublicKey = new File(pubKeyPath);
        FileInputStream fis = new FileInputStream(filePublicKey);
        byte[] encodedPublicKey = new byte[(int) filePublicKey.length()];
        fis.read(encodedPublicKey);
        fis.close();

        // load private
        File filePrivateKey = new File(priKeyPath);
        fis = new FileInputStream(filePrivateKey);
        byte[] encodedPrivateKey = new byte[(int) filePrivateKey.length()];
        fis.read(encodedPrivateKey);
        fis.close();

        // build RSA KeyPair
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(encodedPublicKey);
        PublicKey publicKey = keyFactory.generatePublic(publicKeySpec);

        PKCS8EncodedKeySpec privateKeySpec = new PKCS8EncodedKeySpec(encodedPrivateKey);
        PrivateKey privateKey = keyFactory.generatePrivate(privateKeySpec);

        return new KeyPair(publicKey, privateKey);
    }

    public static void buildAndSaveAsymKeyPair(String pubKeyPath, String priKeyPath) throws IOException, NoSuchAlgorithmException {
        KeyPairGenerator keyGenerator = KeyPairGenerator.getInstance("RSA");
        SecureRandom srand = new SecureRandom();
        keyGenerator.initialize(1024, srand);
        KeyPair keyPair = keyGenerator.generateKeyPair();
        PrivateKey privateKey = keyPair.getPrivate();
        PublicKey publicKey = keyPair.getPublic();

        X509EncodedKeySpec x509EncodedKeySpec = new X509EncodedKeySpec(publicKey.getEncoded());
        FileOutputStream fos = new FileOutputStream(pubKeyPath);
        fos.write(x509EncodedKeySpec.getEncoded());
        fos.close();

        PKCS8EncodedKeySpec pkcs8EncodedKeySpec = new PKCS8EncodedKeySpec(privateKey.getEncoded());
        fos = new FileOutputStream(priKeyPath);
        fos.write(pkcs8EncodedKeySpec.getEncoded());
        fos.close();
    }

    public static Configuration propagateBucketOptions(Configuration source, String bucket) {
        if (null == bucket || 0 == bucket.length()) {
            Preconditions.checkArgument(false, "bucket is null or empty");
        }
        final String bucketPrefix = FS_COSN_BUCKET_PREFIX + bucket +'.';
        LOG.debug("propagating entries under {}", bucketPrefix);
        final Configuration dest = new Configuration(source);
        for (Map.Entry<String, String> entry : source) {
            final String key = entry.getKey();
            final String value = entry.getValue();
            if (!key.startsWith(bucketPrefix) || bucketPrefix.equals(key)) {
                continue;
            }
            //  bucket prefix to strip it
            final String stripped = key.substring(bucketPrefix.length());
            if ("impl".equals(stripped)) {
                // tell user off, here for now not skip "bucket." configurations
                // because of cosn original region or endpoint has sub string "bucket"
                // it is better to change all original without sub string "bucket"
                LOG.debug("Ignoring bucket option {}", key);
            }  else {
                // propagate the value, building a new origin field.
                // to track overwrites, the generic key is overwritten even if
                // already matches the new one.
                final String generic = FS_COSN_PREFIX + stripped;
                LOG.debug("Updating {} from origin", generic);
                dest.set(generic, value, key);
            }
        }
        return dest;
    }

    public static String pathToKey(Path path) {
        if (path.toUri().getScheme() != null && path.toUri().getPath().isEmpty()) {
            // allow uris without trailing slash after bucket to refer to root,
            // like cosn://mybucket
            return "";
        }
        if (!path.isAbsolute()) {
            throw new IllegalArgumentException("Path must be absolute: " + path);
        }
        String ret = path.toUri().getPath();
        if (ret.endsWith("/") && (ret.indexOf("/") != ret.length() - 1)) {
            ret = ret.substring(0, ret.length() - 1);
        }
        return ret;
    }

    public static Path keyToPath(String key, String pathDelimiter) {
        if (!key.startsWith(pathDelimiter)) {
            return new Path("/" + key);
        } else {
            return new Path(key);
        }
    }

    public static boolean checkDirectoryRWPermissions(String directoryPath) {
        java.nio.file.Path path = Paths.get(directoryPath);

        // check dir is existed
        if (!java.nio.file.Files.exists(path)) {
	        LOG.error("the directory {} is not exist", directoryPath);
            return false;
        }

        // check the input is a dir
        if (!java.nio.file.Files.isDirectory(path)) {
            LOG.error("the {} is not a directory", directoryPath);
            return false;
        }

        // check read permission
        boolean isReadable = java.nio.file.Files.isReadable(path);
        // check write permission
        boolean isWritable = java.nio.file.Files.isWritable(path);

        return isReadable && isWritable;
    }
}
