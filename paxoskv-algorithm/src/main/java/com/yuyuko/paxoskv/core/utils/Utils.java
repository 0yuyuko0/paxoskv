package com.yuyuko.paxoskv.core.utils;

import java.util.Collection;
import java.util.Collections;

public class Utils {
    public static boolean isEmpty(Collection collection) {
        return collection == null || collection.size() == 0;
    }

    public static boolean notEmpty(Collection collection) {
        return !isEmpty(collection);
    }

    public static boolean isCollectionEquals(Collection a, Collection b) {
        if (isEmpty(a) && isEmpty(b))
            return true;
        if (isEmpty(a) || isEmpty(b))
            return false;
        return a.equals(b);
    }
}
