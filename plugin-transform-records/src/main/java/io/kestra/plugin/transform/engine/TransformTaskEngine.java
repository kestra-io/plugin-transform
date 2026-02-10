package io.kestra.plugin.transform.engine;

import com.amazon.ion.IonStruct;
import io.kestra.plugin.transform.util.TransformException;

import java.util.List;

public interface TransformTaskEngine {
    TransformResult execute(List<IonStruct> records) throws TransformException;
}
