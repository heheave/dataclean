package javaclz.persist.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableFactory;

import java.io.*;

/**
 * Created by xiaoke on 17-6-19.
 */
public class HConf implements Serializable{

    private transient volatile Configuration conf = new Configuration();

    public HConf(Configuration conf) {
        this.conf = conf;
    }

    private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
        ois.defaultReadObject();
        boolean isNull = ois.readBoolean();
        if (!isNull) {
            if (conf == null) {
                conf = new Configuration();
            }
            conf.readFields(ois);
        } else {
            conf = null;
        }
    }

    private void writeObject(ObjectOutputStream oos) throws IOException {
        oos.defaultWriteObject();
        if (conf != null) {
            oos.writeBoolean(false);
            conf.write(oos);
        } else {
            oos.writeBoolean(true);
        }
    }

    public Configuration hconf() {
        return conf;
    }
}
