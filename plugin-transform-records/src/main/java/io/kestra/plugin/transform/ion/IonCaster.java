package io.kestra.plugin.transform.ion;

import com.amazon.ion.IonValue;

public interface IonCaster {
    IonValue cast(IonValue value, IonTypeName targetType) throws CastException;
}
