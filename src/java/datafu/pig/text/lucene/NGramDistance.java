/*
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package datafu.pig.text.lucene;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * The Lucene NGramDistance class returns a similarity FLOAT between 0-1, given two strings.
 * <p>
 * Example:
 * <pre>
 * {@code
 * define NGramDistance datafu.pig.text.lucene.NGramDistance();
 *
 * -- input:
 * -- ("foobar", "foogoo")
 * infoo = LOAD 'input' AS (word1:chararray, word2:chararray);

 * -- output:
 * -- (0.55152313)
 * outfoo = FOREACH input GENERATE NGramDistance(text) as distance;
 * }
 * </pre>
 */
public class NGramDistance extends EvalFunc<Float>
{
    Integer size = null;

    public NGramDistance() { }

    public NGramDistance(String size) {
        this.size = Integer.valueOf(size);
    }

    public Float exec(Tuple input) throws IOException
    {
        String inputString = null;

        if(input.size() == 0) {
            return null;
        }
        if(input.size() < 2) {
            throw new RuntimeException("Must supply two arguments, got < 2");
        }

        // Accept numerical types or bytes too. Why not?
        String word1 = input.get(0).toString();
        String word2 = input.get(1).toString();

        org.apache.lucene.search.spell.NGramDistance distanceChecker;
        if(this.size == null) {
            distanceChecker = new org.apache.lucene.search.spell.NGramDistance();
        }
        else {
            distanceChecker = new org.apache.lucene.search.spell.NGramDistance(size);
        }

        Float distance = distanceChecker.getDistance(word1, word2);
        return distance;
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.FLOAT));
    }
}
