package rpc.common;

import net.sf.json.JSONObject;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Created by xiaoke on 17-10-19.
 */
public class Args implements Serializable{

    private final RMType rmType;

    private final HashMap<String, String> attrs;

    public Args(RMType rmType) {
        this.rmType = rmType;
        this.attrs = new HashMap<>();
    }

    public void add(String key, String value) {
        attrs.put(key, value);
    }

    public RMType getRmType() {
        return rmType;
    }

    public String getString(String key) {
        return attrs.get(key);
    }

    public boolean getBoolean(String key) {
        String str = attrs.get(key);
        if (str == null) {
            throw new NullPointerException("No boolean attr named " + key);
        } else {
            return Boolean.parseBoolean(str);
        }
    }

    public int getInt(String key) {
        String str = attrs.get(key);
        if (str == null) {
            throw new NullPointerException("No integer attr named " + key);
        } else {
            return Integer.parseInt(str);
        }
    }

    public long getLong(String key) {
        String str = attrs.get(key);
        if (str == null) {
            throw new NullPointerException("No long attr named " + key);
        } else {
            return Long.parseLong(str);
        }
    }

    public double getFloat(String key) {
        String str = attrs.get(key);
        if (str == null) {
            throw new NullPointerException("No float attr named " + key);
        } else {
            return Float.parseFloat(str);
        }
    }

    public double getDouble(String key) {
        String str = attrs.get(key);
        if (str == null) {
            throw new NullPointerException("No double attr named " + key);
        } else {
            return Double.parseDouble(str);
        }
    }

    public HashMap<String, String> getAttrs() {
        return attrs;
    }
}
