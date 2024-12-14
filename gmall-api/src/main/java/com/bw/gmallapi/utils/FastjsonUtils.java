package com.bw.gmallapi.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.fastjson.serializer.SimpleDateFormatSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.*;


@Slf4j
public final class FastjsonUtils {

    //fastjson格式转换器
    private static SerializeConfig config;

    static {
        config = new SerializeConfig();
        config.put(Date.class, new SimpleDateFormatSerializer("yyyy-MM-dd HH:mm:ss"));
    }

    private static final SerializerFeature[] features = {
            // 输出空置字段
            SerializerFeature.WriteMapNullValue,
            // list字段如果为null，输出为[]，而不是null
            SerializerFeature.WriteNullListAsEmpty,
            // 数值字段如果为null，输出为0，而不是null
            SerializerFeature.WriteNullNumberAsZero,
            // Boolean字段如果为null，输出为false，而不是null
            SerializerFeature.WriteNullBooleanAsFalse,
            // 字符类型字段如果为null，输出为""，而不是null
            SerializerFeature.WriteNullStringAsEmpty
    };

    /**
     * Object TO Json String 字符串输出
     */
    public static String toJSONString(Object object) {
        try {
            return JSON.toJSONString(object, config, features);
        } catch (Exception e) {
            log.error("JsonUtil | method=toJSON() | 对象转为Json字符串 Error！" + e.getMessage(), e);
        }
        return null;
    }

    /**
     * Object TO Json String Json-lib兼容的日期输出格式
     */
    public static String toJSONLib(Object object) {
        try {
            return JSON.toJSONString(object, config, features);
        } catch (Exception e) {
            log.error("JsonUtil | method=toJSONLib() | 对象转为Json字符串 Json-lib兼容的日期输出格式   Error！" + e.getMessage(), e);
        }
        return null;
    }

    /**
     * 转换为数组 Object
     */
    public static Object[] toArray(String text) {
        try {
            return toArray(text, null);
        } catch (Exception e) {
            log.error("JsonUtil | method=toArray() | 将json格式的数据转换为数组 Object  Error！" + e.getMessage(), e);
        }
        return null;
    }

    /**
     * 转换为数组 （可指定类型）
     */
    public static <T> Object[] toArray(String text, Class<T> clazz) {
        try {
            return JSON.parseArray(text, clazz).toArray();
        } catch (Exception e) {
            log.error("JsonUtil | method=toArray() | 将json格式的数据转换为数组 （可指定类型）   Error！" + e.getMessage(), e);
        }
        return null;
    }

    /**
     * List转换为JSONArray
     */
    public static JSONArray toJSONArray(List<?> list) {
        try {
            String json = toJSONString(list);
            System.out.println(json);
            return JSONArray.parseArray(json);
        } catch (Exception e) {
            log.error("JsonUtil | method=toJSONArray() | 将list格式的数据转换为JSONArray Object  Error！" + e.getMessage(), e);
        }
        return null;
    }

    /**
     * 重载类型为String
     * Json 转为 Jave Bean, 再使用Bean类型进行强转
     */
    public static <T> T toBean(String text, Class<T> clazz) {
        try {
            return JSON.parseObject(text, clazz);
        } catch (Exception e) {
            log.error("JsonUtil | method=toBean() | Json 转为  Jave Bean  Error！" + e.getMessage(), e);
        }
        return null;
    }

    /**
     * 重载类型为JSONObject
     * Json 转为 Jave Bean, 再使用Bean类型进行强转
     */
    public static <T> T toBean(JSONObject text, Class<T> clazz) {
        try {
            String json = toJSONString(text);
            return toBean(json, clazz);
        } catch (Exception e) {
            log.error("JsonUtil | method=toBean() | Json 转为  Jave Bean  Error！" + e.getMessage(), e);
        }
        return null;
    }


    /**
     * 重载类型为String
     * Json 转为 Map，JSON.parseObject转换的类型为JSONObject，但是JSONObject实现了Map接口
     */
    public static Map<?, ?> toMap(String json) {
        try {
            return JSON.parseObject(json);
        } catch (Exception e) {
            log.error("JsonUtil | method=toMap() | Json 转为   Map {},{}" + e.getMessage(), e);
        }
        return null;
    }

    /**
     * 重载类型为JSONObject
     * Json 转为 Map，JSON.parseObject转换的类型为JSONObject，但是JSONObject实现了Map接口
     */
    public static Map<?, ?> toMap(JSONObject json) {
        try {
            return JSON.parseObject(JSON.toJSONString(json));
        } catch (Exception e) {
            log.error("JsonUtil | method=toMap() | Json 转为   Map {},{}" + e.getMessage(), e);
        }
        return null;
    }


    /**
     * 重载类型为String
     * Json 转为 Map，JSON.parseObject转换的类型为JSONObject，但是JSONObject实现了Map接口
     */
    public static HashMap<?, ?> toHashMap(String json) {
        try {
            Map<?, ?> map = toMap(json);
            return new HashMap<>(map);
        } catch (Exception e) {
            log.error("JsonUtil | method=toMap() | Json 转为   Map {},{}" + e.getMessage(), e);
        }
        return null;
    }

    /**
     * 重载类型为JSONObject
     * Json 转为 Map，JSON.parseObject转换的类型为JSONObject，但是JSONObject实现了Map接口
     */
    public static HashMap<?, ?> toHashMap(JSONObject json) {
        try {
            Map<?, ?> map = toMap(json);
            return new HashMap<>(map);
        } catch (Exception e) {
            log.error("JsonUtil | method=toMap() | Json 转为   Map {},{}" + e.getMessage(), e);
        }
        return null;
    }


    /**
     * 重载类型为String
     * Json 转 List,Class 集合中泛型的类型，非集合本身，可json-lib兼容的日期格式
     */
    public static <T> List<T> toList(String text, Class<T> clazz) {
        try {
            return JSON.parseArray(text, clazz);
        } catch (Exception e) {
            log.error("JsonUtil | method=toList() | Json 转为   List {},{}" + e.getMessage(), e);
        }
        return null;
    }

    /**
     * 重载类型为JSONObject
     * Json 转 List,Class 集合中泛型的类型，非集合本身，可json-lib兼容的日期格式
     */
    public static <T> List<T> toList(JSONArray text, Class<T> clazz) {
        try {
            String json = toJSONString(text);
            return toList(json, clazz);
        } catch (Exception e) {
            log.error("JsonUtil | method=toList() | Json 转为   List {},{}" + e.getMessage(), e);
        }
        return null;
    }


    /**
     * 重载类型为String
     * Json 转 List,Class 集合中泛型的类型，非集合本身，可json-lib兼容的日期格式
     */
    public static <T> ArrayList<T> toArrayList(String text, Class<T> clazz) {
        try {
            List<T> list = toList(text, clazz);
            assert list != null;
            return new ArrayList<>(list);
        } catch (Exception e) {
            log.error("JsonUtil | method=toList() | Json 转为   List {},{}" + e.getMessage(), e);
        }
        return null;
    }

    /**
     * 重载类型为JSONObject,clazz为list里面的类型
     * Json 转 List,Class 集合中泛型的类型，非集合本身，可json-lib兼容的日期格式
     */
    public static <T> ArrayList<T> toArrayList(JSONArray text, Class<T> clazz) {
        try {
            List<T> list = toList(text, clazz);
            assert list != null;
            return new ArrayList<>(list);
        } catch (Exception e) {
            log.error("JsonUtil | method=toList() | Json 转为   List {},{}" + e.getMessage(), e);
        }
        return null;
    }

    /**
     * 重载类型为String
     * 从json字符串获取指定key的字符串
     */
    public static Object getValueFromJson(final String json, final String key) {
        try {
            if (StringUtils.isBlank(json) || StringUtils.isBlank(key)) {
                return null;
            }
            return JSON.parseObject(json).getString(key);
        } catch (Exception e) {
            log.error("JsonUtil | method=getStringFromJson() | 从json获取指定key的字符串 Error！" + e.getMessage(), e);
        }
        return null;
    }

    /**
     * 重载类型为JSONObject
     * 从json字符串获取指定key的字符串
     */
    public static Object getValueFromJson(final JSONObject json, final String key) {
        try {
            String text = toJSONString(json);
            return getValueFromJson(text, key);
        } catch (Exception e) {
            log.error("JsonUtil | method=getStringFromJson() | 从json获取指定key的字符串 Error！" + e.getMessage(), e);
        }
        return null;
    }
}