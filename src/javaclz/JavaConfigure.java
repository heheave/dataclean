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

    public String getStringOrElse(Mark<String> mark) {
        String e = mapGet(mark.key);
        if (e != null) {
            return e;
        } else {
            return mark.dv();
        }
    }

    public String getStringOrElse(Mark<String> mark, String str) {
        String e = mapGet(mark.key);
        if (e != null) {
            return e;
        } else {
            return str;
        }
    }

    public boolean getBooleanOrElse(Mark<Boolean> mark) {
        try {
            String e = mapGet(mark.key);
            if (e != null) {
                return Boolean.parseBoolean(e.toString());
            } else {
                return mark.dv();
            }
        } catch (NumberFormatException e) {
            e.printStackTrace();
            return mark.dv();
        }
    }

    public boolean getBooleanOrElse(Mark<Boolean> mark, boolean b) {
        try {
            String e = mapGet(mark.key);
            if (e != null) {
                return Boolean.parseBoolean(e.toString());
            } else {
                return b;
            }
        } catch (NumberFormatException e) {
            e.printStackTrace();
            return b;
        }
    }

    public int getIntOrElse(Mark<Integer> mark) {
        try {
            String e = mapGet(mark.key);
            if (e != null) {
                return Integer.parseInt(e.toString());
            } else {
                return mark.dv();
            }
        } catch (NumberFormatException e) {
            e.printStackTrace();
            return mark.dv();
        }
    }

    public int getIntOrElse(Mark<Integer> mark, int i) {
        try {
            String e = mapGet(mark.key);
            if (e != null) {
                return Integer.parseInt(e.toString());
            } else {
                return i;
            }
        } catch (NumberFormatException e) {
            e.printStackTrace();
            return i;
        }
    }

    public long getLongOrElse(Mark<Long> mark) {
        try {
            String e = mapGet(mark.key);
            if (e != null) {
                return Long.parseLong(e.toString());
            } else {
                return mark.dv();
            }
        } catch (NumberFormatException e) {
            e.printStackTrace();
            return mark.dv();
        }
    }

    public long getLongOrElse(Mark<Long> mark, long l) {
        try {
            String e = mapGet(mark.key);
            if (e != null) {
                return Long.parseLong(e.toString());
            } else {
                return l;
            }
        } catch (NumberFormatException e) {
            e.printStackTrace();
            return l;
        }
    }

    public float getFloatOrElse(Mark<Float> mark) {
        try {
            String e = mapGet(mark.key);
            if (e != null) {
                return Float.parseFloat(e.toString());
            } else {
                return mark.dv();
            }
        } catch (NumberFormatException e) {
            e.printStackTrace();
            return mark.dv();
        }
    }

    public float getFloatOrElse(Mark<Float> mark, float f) {
        try {
            String e = mapGet(mark.key);
            if (e != null) {
                return Float.parseFloat(e.toString());
            } else {
                return f;
            }
        } catch (NumberFormatException e) {
            e.printStackTrace();
            return f;
        }
    }

    public double getDoubleOrElse(Mark<Double> mark) {
        try {
            String e = mapGet(mark.key);
            if (e != null) {
                return Double.parseDouble(e.toString());
            } else {
                return mark.dv();
            }
        } catch (NumberFormatException e) {
            e.printStackTrace();
            return mark.dv();
        }
    }

    public double getDoubleOrElse(Mark<Double> mark, double d) {
        try {
            String e = mapGet(mark.key);
            if (e != null) {
                return Double.parseDouble(e.toString());
            } else {
                return d;
            }
        } catch (NumberFormatException e) {
            e.printStackTrace();
            return d;
        }
    }
}
