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

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;

/**
 * The OpenNLP Tokenizers segment an input character sequence into tokens.
 * <p>
 * Example:
 * <pre>
 * {@code
 * define TokenizeME datafu.pig.text.TokenizeME();
 *
 * -- input:
 * -- ("I believe the Masons have infiltrated the Apache PMC.")
 * infoo = LOAD 'input' AS (text:chararray);

 * -- output:
 * -- ({(I),(believe),(the),(Masons),(have),(infiltrated),(the),(Apache),(PMC),(.)})
 * outfoo = FOREACH input GENERATE TokenizeME(text) as tokens;
 * }
 * </pre>
 */



public class TokenizeME extends EvalFunc<DataBag>
{
    private boolean isFirst = true;
    InputStream is = null;
    TokenizerModel model = null;
    private TokenizerME tokenizer = null;
    public static final String MODEL_FILE = "tokens";
    TupleFactory tf = TupleFactory.getInstance();
    BagFactory bf = BagFactory.getInstance();
    String modelPath;

    public TokenizeME(String modelPath) {
        this.modelPath = modelPath;
    }

    public TokenizeME() {
        this.modelPath = "data/en-token.bin";
    }

    @Override
    public List<String> getCacheFiles() {
        List<String> list = new ArrayList<String>(1);
        list.add(this.modelPath + "#" + MODEL_FILE);
        return list;
    }

    private String getFilename() throws IOException {
        // if the symlink exists, use it, if not, use the raw name if it exists
        // note: this is to help with testing, as it seems distributed cache doesn't work with PigUnit
        String loadFile = MODEL_FILE;
        if (!new File(loadFile).exists()) {
            if (new File(this.modelPath).exists()) {
                loadFile = this.modelPath;
            } else {
                throw new IOException(String.format("Could not load model, neither symlink %s nor file %s exist", MODEL_FILE, this.modelPath));
            }
        }
        return loadFile;
    }

    // Enable multiple languages by specifying the model path. See http://text.sourceforge.net/models-1.5/
    public DataBag exec(Tuple input) throws IOException
    {
        String inputString = null;

        if(input.size() == 0) {
            return null;
        }
        if(input.size() == 1) {
            inputString = input.get(0).toString();

        if(inputString == null || inputString == "") {
            return null;
        }

        DataBag outBag = bf.newDefaultBag();
        if(isFirst == true || this.tokenizer == null) {
            String loadFile = getFilename();
            InputStream file = new FileInputStream(loadFile);
            InputStream buffer = new BufferedInputStream(file);
            TokenizerModel model = new TokenizerModel(buffer);
            this.tokenizer = new TokenizerME(model);

            isFirst = false;
        }
        String tokens[] = this.tokenizer.tokenize(inputString);
        for(String token : tokens) {
            Tuple outTuple = tf.newTuple(token);
            outBag.add(outTuple);
        }
        return outBag;
    }
}
