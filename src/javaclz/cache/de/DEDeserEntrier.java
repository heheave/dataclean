package javaclz.cache.de;

import javaclz.cache.CEntry;
import javaclz.cache.DeserEntrier;
import net.sf.json.JSONArray;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by xiaoke on 17-11-24.
 */
public class DEDeserEntrier implements DeserEntrier {
    public CEntry fromBytes(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        JSONArray ja = JSONArray.fromObject(new String(bytes));
        final List<DE> res = new ArrayList<DE>(ja.size());
        for (int i = 0; i < ja.size(); i++) {
            res.add(new DE(ja.getJSONObject(i)));
        }
        return new CEntry() {
            public Object getAttach() {
                return res;
            }
        };
    }
}
