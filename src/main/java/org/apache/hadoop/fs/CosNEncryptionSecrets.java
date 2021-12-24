package org.apache.hadoop.fs;

import com.qcloud.cos.utils.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.*;
import java.util.Objects;


public class CosNEncryptionSecrets implements Writable, Serializable {

    public static final int MAX_SECRET_LENGTH = 2048;

    private static final long serialVersionUID = 1208329045511296375L;

    /**
     * Encryption algorithm to use: must match one in
     * {@link CosNEncryptionMethods}.
     */
    private String encryptionAlgorithm = "";

    /**
     * Encryption key: possibly sensitive information.
     */
    private String encryptionKey = "";

    /**
     * Encryption context used by sse-kms
     */
    private String encryptionContext = "";

    /**
     * This field isn't serialized/marshalled; it is rebuilt from the
     * encryptionAlgorithm field.
     */
    private transient CosNEncryptionMethods encryptionMethod =
            CosNEncryptionMethods.NONE;

    /**
     * Empty constructor, for use in marshalling.
     */
    public CosNEncryptionSecrets() {
    }

    /**
     * Create a pair of secrets.
     *
     * @param encryptionAlgorithm algorithm enumeration.
     * @param encryptionKey       key/key reference.
     * @throws IOException failure to initialize.
     */
    public CosNEncryptionSecrets(final CosNEncryptionMethods encryptionAlgorithm,
                                 final String encryptionKey) throws IOException {
        this(encryptionAlgorithm.getMethod(), encryptionKey);
    }

    public CosNEncryptionSecrets(final CosNEncryptionMethods encryptionAlgorithm,
                                 final String encryptionKey, final String encryptionContext) throws IOException {
        this(encryptionAlgorithm.getMethod(), encryptionKey, encryptionContext);
    }
    /**
     * Create a pair of secrets.
     *
     * @param encryptionAlgorithm algorithm name
     * @param encryptionKey       key/key reference.
     * @throws IOException failure to initialize.
     */
    public CosNEncryptionSecrets(final String encryptionAlgorithm,
                                 final String encryptionKey) throws IOException {
        this.encryptionAlgorithm = encryptionAlgorithm;
        this.encryptionKey = encryptionKey;
        init();
    }

    public CosNEncryptionSecrets(final String encryptionAlgorithm,
                                 final String encryptionKey, final String encryptionContext) throws IOException {
        this.encryptionAlgorithm = encryptionAlgorithm;
        this.encryptionKey = encryptionKey;
        this.encryptionContext = encryptionContext;
        init();
    }
    /**
     * Write out the encryption secrets.
     *
     * @param out {@code DataOutput} to serialize this object into.
     * @throws IOException IO failure
     */
    @Override
    public void write(final DataOutput out) throws IOException {
        new LongWritable(serialVersionUID).write(out);
        Text.writeString(out, encryptionAlgorithm);
        Text.writeString(out, encryptionKey);
    }

    /**
     * Read in from the writable stream.
     * After reading, call {@link #init()}.
     *
     * @param in {@code DataInput} to deserialize this object from.
     * @throws IOException failure to read/validate data.
     */
    @Override
    public void readFields(final DataInput in) throws IOException {
        final LongWritable version = new LongWritable();
        version.readFields(in);
        if (version.get() != serialVersionUID) {
            throw new IOException(
                    "Incompatible EncryptionSecrets version");
        }
        encryptionAlgorithm = Text.readString(in, MAX_SECRET_LENGTH);
        encryptionKey = Text.readString(in, MAX_SECRET_LENGTH);
        init();
    }

    /**
     * For java serialization: read and then call {@link #init()}.
     *
     * @param in input
     * @throws IOException            IO problem
     * @throws ClassNotFoundException problem loading inner class.
     */
    private void readObject(ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        init();
    }

    /**
     * Init all state, including after any read.
     *
     * @throws IOException error rebuilding state.
     */
    private void init() throws IOException {
        encryptionMethod = CosNEncryptionMethods.getMethod(
                encryptionAlgorithm);
    }

    public String getEncryptionAlgorithm() {
        return encryptionAlgorithm;
    }

    public String getEncryptionKey() {
        return encryptionKey;
    }

    public String getEncryptionContext() { return encryptionContext; }
    /**
     * Does this instance have encryption options?
     * That is: is the algorithm non-null.
     *
     * @return true if there's an encryption algorithm.
     */
    public boolean hasEncryptionAlgorithm() {
        return !StringUtils.isNullOrEmpty(encryptionAlgorithm);
    }

    /**
     * Does this instance have an encryption key?
     *
     * @return true if there's an encryption key.
     */
    public boolean hasEncryptionKey() {
        return !StringUtils.isNullOrEmpty(encryptionKey);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final CosNEncryptionSecrets that = (CosNEncryptionSecrets) o;
        return Objects.equals(encryptionAlgorithm, that.encryptionAlgorithm)
                && Objects.equals(encryptionKey, that.encryptionKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(encryptionAlgorithm, encryptionKey);
    }

    /**
     * Get the encryption method.
     *
     * @return the encryption method
     */
    public CosNEncryptionMethods getEncryptionMethod() {
        return encryptionMethod;
    }

    /**
     * String function returns the encryption mode but not any other
     * secrets.
     *
     * @return a string safe for logging.
     */
    @Override
    public String toString() {
        return CosNEncryptionMethods.NONE.equals(encryptionMethod)
                ? "(no encryption)"
                : encryptionMethod.getMethod();
    }
}
