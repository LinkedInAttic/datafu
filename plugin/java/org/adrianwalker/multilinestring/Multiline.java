// Code from https://github.com/benelog/multiline.git
// Based on Adrian Walker's blog post: http://www.adrianwalker.org/2011/12/java-multiline-string.html

package org.adrianwalker.multilinestring;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.FIELD,ElementType.LOCAL_VARIABLE})
@Retention(RetentionPolicy.SOURCE)
public @interface Multiline {
}