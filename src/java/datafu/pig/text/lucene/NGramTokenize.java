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

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttributeImpl;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

public class NGramTokenize extends EvalFunc<DataBag> {

    BagFactory bf = BagFactory.getInstance();
    TupleFactory tf = TupleFactory.getInstance();

    public DataBag exec(Tuple input) throws IOException
    {
        String inputString = null;
        DataBag outBag = bf.newDefaultBag();

        if(input.size() == 0) {
            return null;
        }
        inputString = input.get(0).toString();
        Reader reader = new StringReader(inputString);
        TokenStream tokenizer = new StandardTokenizer(Version.LUCENE_36, reader);
        tokenizer = new ShingleFilter(tokenizer, 1, 3);
        CharTermAttribute charTermAttribute = tokenizer.addAttribute(CharTermAttribute.class);

        while (tokenizer.incrementToken()) {
            String token = charTermAttribute.toString();
            Tuple newTuple = tf.newTuple(1);
            newTuple.set(0, token);
            outBag.add(newTuple);
        }
        return outBag;
    }

    @Override
    public Schema outputSchema(Schema input)
    {
        try
        {
            Schema.FieldSchema inputFieldSchema = input.getField(0);

            if (inputFieldSchema.type != DataType.CHARARRAY)
            {
                throw new RuntimeException("Expected a CHARARRAY as input, but got a " + inputFieldSchema.toString());
            }

            Schema tupleSchema = new Schema();
            tupleSchema.add(new Schema.FieldSchema("ngram",DataType.CHARARRAY));

            return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass()
                    .getName()
                    .toLowerCase(), input),
                    tupleSchema,
                    DataType.BAG));
        }
        catch (FrontendException e)
        {
            throw new RuntimeException(e);
        }
    }
}
