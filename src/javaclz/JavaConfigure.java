package javaclz;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by xiaoke on 17-5-6.
 */
public class JavaConfigure {

    private static final String V_XML_PATH = JavaV.VAR_FILE_PATH;

    private Map<String, String> XML_VAR_MAP = null;

    public void readFromXml(String filePath) throws DocumentException {
        check();
        synchronized (this) {
            XML_VAR_MAP.clear();
            File f = new File(filePath);
            SAXReader reader = new SAXReader();
            Document doc = reader.read(f);
            Element root = doc.getRootElement();
            List<?> vs = root.elements("v");
            for (Object obj : vs) {
                if (obj instanceof Element) {
                    Element ve = (Element) obj;
                    String name = ve.attributeValue("name");
                    Object value = ve.getData();
                    if (name != null) {
                        XML_VAR_MAP.put(name.trim(), value.toString().trim());
                    }
                }
            }
        }
    }

    public void readFromXml() throws DocumentException {
        readFromXml(V_XML_PATH);
    }

    private void check() {
        if (XML_VAR_MAP == null) {
            synchronized (this) {
                if (XML_VAR_MAP == null) {
                    XML_VAR_MAP = new HashMap<String, String>();
                }
            }
        }
    }

    private String mapGet(String key) {
        check();
        return XML_VAR_MAP.get(key);
    }
    
    public String getString(String key) {
        return mapGet(key);
    }

    public String getStringOrElse(String key, String defualt) {
        String e = mapGet(key);
        if (e != null) {
            return e;
        } else {
            return defualt;
        }
    }

    public boolean getBoolean(String key) {
        return Boolean.parseBoolean(XML_VAR_MAP.get(key));
    }

    public boolean getBooleanOrElse(String key, boolean defualt) {
        try {
            String e = mapGet(key);
            if (e != null) {
                return Boolean.parseBoolean(e.toString());
            } else {
                return defualt;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return defualt;
        }
    }

    public int getInt(String key) {
        return Integer.parseInt(XML_VAR_MAP.get(key));
    }

    public int getIntOrElse(String key, int defualt) {
        try {
            String e = mapGet(key);
            if (e != null) {
                return Integer.parseInt(e.toString());
            } else {
                return defualt;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return defualt;
        }
    }

    public long getLong(String key) {
        return Long.parseLong(XML_VAR_MAP.get(key));
    }

    public long getLongOrElse(String key, long defualt) {
        try {
            String e = mapGet(key);
            if (e != null) {
                return Long.parseLong(e.toString());
            } else {
                return defualt;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return defualt;
        }
    }

    public float getFloat(String key) {
        return Float.parseFloat(XML_VAR_MAP.get(key));
    }

    public float getFloatOrElse(String key, float defualt) {
        try {
            String e = mapGet(key);
            if (e != null) {
                return Float.parseFloat(e.toString());
            } else {
                return defualt;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return defualt;
        }
    }


    public double getDouble(String key) {
        return Double.parseDouble(XML_VAR_MAP.get(key));
    }

    public double getDoubleOrElse(String key, double defualt) {
        try {
            String e = mapGet(key);
            if (e != null) {
                return Double.parseDouble(e.toString());
            } else {
                return defualt;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return defualt;
        }
    }
}
