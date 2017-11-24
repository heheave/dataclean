package rpc.common;

import java.io.Serializable;

/**
 * Created by xiaoke on 17-10-19.
 */
public class Ret implements Serializable{

    private final RTCode code;

    private final int len;

    private final String[] detail;

    public Ret(RTCode code, String[] detail) {
        this.code = code;
        if (detail == null) {
            len = 0;
        } else {
            len = detail.length;
        }
        this.detail = detail;
    }

    public RTCode getCode() {
        return code;
    }

    public int getLen() {
        return len;
    }

    public String[] getDetail() {
        return detail;
    }

    public static Ret s(String str) {
        return Ret.s(new String[]{str});
    }

    public static Ret s(String[] str) {
        return new Ret(RTCode.S, str);
    }

    public static Ret f(String str) {
        return Ret.f(new String[]{str});
    }

    public static Ret f(String[] str) {
        return new Ret(RTCode.F, str);
    }

    public static Ret e(String str) {
        return Ret.e(new String[]{str});
    }

    public static Ret e(String[] str) {
        return new Ret(RTCode.E, str);
    }
}
