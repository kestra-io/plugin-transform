package io.kestra.plugin.transform.engine;

import com.amazon.ion.IonStruct;
import io.kestra.plugin.transform.util.TransformException;

public interface RecordTransformer {
    IonStruct transform(IonStruct input) throws TransformException;
}
