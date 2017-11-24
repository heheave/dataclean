package javaclz.cache.de;

import avgcache.Avg;
import conf.deviceconfig.action.Actions;
import conf.deviceconfig.action.expression.Expr;
import conf.deviceconfig.action.expression.ExprUtil;
import net.sf.json.JSONObject;
import scala.Option;

/**
 * Created by xiaoke on 17-11-23.
 */
public class DE {

    private final int uid;
    private final int aid;
    private final int sid;
    private final int pid;
    private final String poutunit;
    private final Option<Avg[]> avgs;
    private final Expr expr;

    public DE(JSONObject jo) {
        uid = !jo.containsKey("uid") ? 0: jo.getInt("uid");
        aid = !jo.containsKey("aid") ? 0: jo.getInt("aid");
        sid = !jo.containsKey("sid") ? 0: jo.getInt("sid");
        pid = !jo.containsKey("pid") ? 0: jo.getInt("pid");
        //pcalculate = jo.containsKey("pcalculate") ? null: jo.getString("pcalculate");
        poutunit = !jo.containsKey("poutunit") ? null: jo.getString("poutunit");
        avgs = !jo.containsKey("pavg") ? Actions.getActions(null) : Actions.getActions(jo.getString("pavg"));
        expr = !jo.containsKey("pcalculate") ? null: ExprUtil.fromString(jo.getString("pcalculate"));
    }

    public int getUid() {
        return uid;
    }

    public int getAid() {
        return aid;
    }

    public int getSid() {
        return sid;
    }

    public int getPid() {
        return pid;
    }

    public String getPoutunit() {
        return poutunit;
    }

    public Option<Avg[]> getAvgs() {
        return avgs;
    }

    public Expr getExpr() {
        return expr;
    }
}
