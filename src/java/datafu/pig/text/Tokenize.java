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
package datafu.pig.text;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import org.apache.pig.data.*;

import datafu.pig.util.SimpleEvalFunc;

/**
 * The OpenNLP Tokenizers segment an input character sequence into tokens.
 * <p>
 * Example:
 * <pre>
 * {@code
 * define Tokenize datafu.pig.bags.Tokenize();
 *
 * -- input:
 * -- ("I believe the Masons have infiltrated the Apache PMC.")
 * infoo = LOAD 'input' AS (text:chararray);

 * -- output:
 * -- ({(I),(believe),(the),(Masons),(have),(infiltrated),(the),(Apache),(PMC)})
 * outfoo = FOREACH input GENERATE Tokenize(text) as tokens;
 * }
 * </pre>
 */
public class Tokenize extends SimpleEvalFunc<DataBag>
{
    private static boolean isFirst = true;
    InputStream is = null;
    TokenizerModel model = null;
    TokenizerME tokenizer = null;
    TupleFactory tf = TupleFactory.getInstance();
    BagFactory bf = BagFactory.getInstance();

    public DataBag call(String inputString) throws IOException
    {
        DataBag outBag = bf.newDefaultBag();
        if(isFirst == true) {
            is = new FileInputStream("en-token.bin");
            model = new TokenizerModel(is);
            tokenizer = new TokenizerME(model);

            isFirst = false;
        }
        String tokens[] = tokenizer.tokenize(inputString);
        for(String token : tokens) {
            Tuple outTuple = tf.newTuple(token);
            outBag.add(outTuple);
        }
        return outBag;
    }
}
