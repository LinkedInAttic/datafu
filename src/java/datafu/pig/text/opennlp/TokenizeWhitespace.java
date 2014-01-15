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
package datafu.pig.text.opennlp;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import opennlp.tools.tokenize.WhitespaceTokenizer;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * The OpenNLP Tokenizers segment an input character sequence into tokens.
 * <p>
 * Example:
 * <pre>
 * {@code
 * define TokenizeWhitespace datafu.pig.text.TokenizeWhitespace();
 *
 * -- input:
 * -- ("I believe the Masons have infiltrated the Apache PMC.")
 * infoo = LOAD 'input' AS (text:chararray);

 * -- output:
 * -- ({(I),(believe),(the),(Masons),(have),(infiltrated),(the),(Apache),(PMC),(.)})
 * outfoo = FOREACH input GENERATE TokenizeWhitespace(text) as tokens;
 * }
 * </pre>
 */
public class TokenizeWhitespace extends EvalFunc<DataBag>
{
    private boolean isFirst = true;
    InputStream is = null;
    WhitespaceTokenizer tokenizer = WhitespaceTokenizer.INSTANCE;
    TupleFactory tf = TupleFactory.getInstance();
    BagFactory bf = BagFactory.getInstance();

    public DataBag exec(Tuple input) throws IOException
    {
        String inputString = null;

        if(input.size() == 0) {
            return null;
        }
        if(input.size() == 1) {
            inputString = input.get(0).toString();
        }

        if(inputString == null || inputString == "") {
            return null;
        }

        DataBag outBag = bf.newDefaultBag();
        String tokens[] = tokenizer.tokenize(inputString);
        for(String token : tokens) {
            Tuple outTuple = tf.newTuple(token);
            outBag.add(outTuple);
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
            tupleSchema.add(new Schema.FieldSchema("token",DataType.CHARARRAY));

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
