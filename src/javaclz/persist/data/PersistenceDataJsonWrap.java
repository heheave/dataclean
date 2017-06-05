package javaclz.persist.data;

import net.sf.json.JSONObject;

/**
 * Created by xiaoke on 17-6-4.
 */
public class PersistenceDataJsonWrap implements PersistenceData{

    private final JSONObject jo;

    public PersistenceDataJsonWrap(JSONObject jo) {
        this.jo = jo;
    }

    @Override
    public JSONObject toJson() {
        return jo;
    }
}
