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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;
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
 * define TokenizeME datafu.pig.text.TokenizeME();
 * define POSTag datafu.pig.text.POSTag();
 *
 * -- input:
 * -- ({(Appetizers),(during),(happy),(hour),(range),(from),($),(3-$),(8+),(.)})
 * infoo = LOAD 'input' AS (text:chararray);

 * -- output:
 * -- Tuple schema is: (word, tag, confidence)
 * outfoo = FOREACH input GENERATE FLATTEN(TokenizeME(text)) AS tokens;
 * -- ({(Appetizers,NNP,0.3619277937390988),(during,IN,0.7945543860326094),(happy,JJ,0.9888504792754391),
 * -- (hour,NN,0.9427455123502427),(range,NN,0.7335527963654751),(from,IN,0.9911576465589752),($,$,0.9652034031895174),
 * -- (3-$,CD,0.7005347487371849),(8+,CD,0.8227771746247106),(.,.,0.9900983495480891)})
 * outfoo2 = FOREACH outfoo GENERATE POSTag(tokens) AS tagged;
 * }
 * </pre>
 */
public class POSTag extends EvalFunc<DataBag>
{
    private boolean isFirst = true;
    InputStream modelIn = null;
    POSModel model = null;
    POSTaggerME tagger = null;
    public static final String MODEL_FILE = "pos";
    TupleFactory tf = TupleFactory.getInstance();
    BagFactory bf = BagFactory.getInstance();
    String modelPath;

    public POSTag(String modelPath) {
        this.modelPath = modelPath;
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
        DataBag inputBag = null;
        String modelPath = "data/en-pos-maxent.bin";

        if(input.size() == 0) {
            return null;
        }
        if(input.size() == 1) {
            inputBag = (DataBag)input.get(0);
        }
        if(input.size() == 2) {
            modelPath = input.get(1).toString();
            inputBag = (DataBag)input.get(0);
        }

        DataBag outBag = bf.newDefaultBag();
        if(isFirst == true) {
            modelIn = new FileInputStream(modelPath);
            model = new POSModel(modelIn);
            this.tagger = new POSTaggerME(model);

            isFirst = false;
        }

        // Form an inputString array thing for tagger to act on
        int bagLength = (int)inputBag.size();
        String[] words = new String[bagLength];

        Iterator<Tuple> itr = inputBag.iterator();
        int i = 0;
        while(itr.hasNext()) {
            words[i] = (String)itr.next().get(0);
            i++;
        }

        // Compute tags and their probabilities
        String tags[] = this.tagger.tag(words);
        double probs[] = this.tagger.probs();

        // Build output bag of 3-tuples
        for(int j = 0; j < tags.length; j++) {
            Tuple newTuple = tf.newTuple(3);
            if(words.length > 0) {
                newTuple.set(0, words[j]);
                newTuple.set(1, tags[j]);
                newTuple.set(2, probs[j]);
                outBag.add(newTuple);
            }
        }

        return outBag;
    }

    @Override
    public Schema outputSchema(Schema input)
    {
        try
        {
            Schema.FieldSchema inputFieldSchema = input.getField(0);

            if (inputFieldSchema.type != DataType.BAG)
            {
                throw new RuntimeException("Expected a BAG as input");
            }

            Schema inputBagSchema = inputFieldSchema.schema;

            if(inputBagSchema == null) {
                return null;
            }

            if (inputBagSchema.getField(0).type != DataType.TUPLE)
            {
                throw new RuntimeException(String.format("Expected input bag to contain a TUPLE, but instead found %s",
                        DataType.findTypeName(inputBagSchema.getField(0).type)));
            }

            Schema inputTupleSchema = inputBagSchema.getField(0).schema;

            if (inputTupleSchema.size() != 1)
            {
                throw new RuntimeException("Expected one field for the token data");
            }

            if (inputTupleSchema.getField(0).type != DataType.CHARARRAY)
            {
                throw new RuntimeException(String.format("Expected source to be a CHARARRAY, but instead found %s",
                        DataType.findTypeName(inputTupleSchema.getField(0).type)));
            }

            Schema tupleSchema = new Schema();
            tupleSchema.add(new Schema.FieldSchema("token",DataType.CHARARRAY));
            tupleSchema.add(new Schema.FieldSchema("tag",DataType.CHARARRAY));
            tupleSchema.add(new Schema.FieldSchema("probability",DataType.DOUBLE));

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
