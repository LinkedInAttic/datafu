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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;

import opennlp.tools.cmdline.postag.POSModelLoader;
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
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
 * -- ("I believe the Masons have infiltrated the Apache PMC. I believe laser beams control cat brains.")
 * infoo = LOAD 'input' AS (text:chararray);

 * -- output:
 * -- ({(I believe the Masons have infiltrated the Apache PMC.)(I believe laser beams control cat brains.)})
 * outfoo = FOREACH input GENERATE SentenceDetect(text) as tokens;
 * }
 * </pre>
 */
public class POSTag extends SimpleEvalFunc<DataBag>
{
    private static boolean isFirst = true;
    InputStream modelIn = null;
    POSModel model = null;
    POSTaggerME tagger = null;
    TupleFactory tf = TupleFactory.getInstance();
    BagFactory bf = BagFactory.getInstance();

    public DataBag call(DataBag inputBag) throws IOException
    {
        DataBag outBag = bf.newDefaultBag();
        if(isFirst == true) {
            modelIn = new FileInputStream("data/en-pos-maxent.bin");
            model = new POSModel(modelIn);
            tagger = new POSTaggerME(model);

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
        String tags[] = tagger.tag(words);
        double probs[] = tagger.probs();

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
}
