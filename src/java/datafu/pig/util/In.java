package datafu.pig.util;

import java.io.IOException;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

/**
 * In has been renamed to InUDF.
 * 
 * This class is provided for backward compatibility.
 *
 * @deprecated Use {@link InUDF()} instead.
 */
 @Deprecated
public class In extends InUDF
{
}
