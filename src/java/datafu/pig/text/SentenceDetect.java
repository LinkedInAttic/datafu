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

import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;

/**
 * The OpenNLP SentenceDectectors segment an input paragraph into sentences.
 * <p>
 * Example:
 * <pre>
 * {@code
 * define SentenceDetect datafu.pig.text.SentenceDetect();
 *
 * -- input:
 * -- ("I believe the Masons have infiltrated the Apache PMC. I believe laser beams control cat brains.")
 * infoo = LOAD 'input' AS (text:chararray);

 * -- output:
 * -- ({(I believe the Masons have infiltrated the Apache PMC.)(I believe laser beams control cat brains.)})
 * outfoo = FOREACH input GENERATE SentenceDetect(text) as sentences;
 * }
 * </pre>
 */
public class SentenceDetect extends EvalFunc<DataBag>
{
    private boolean isFirst = true;
    InputStream is = null;
    SentenceModel model = null;
    SentenceDetectorME sdetector = null;
    TupleFactory tf = TupleFactory.getInstance();
    BagFactory bf = BagFactory.getInstance();

    // Enable multiple languages by specifying the model path. See http://opennlp.sourceforge.net/models-1.5/
    public DataBag exec(Tuple input) throws IOException
    {
        String inputString = null;
        String modelPath = "data/en-sent.bin";

        if(input.size() == 0) {
            return null;
        }
        if(input.size() == 1) {
            inputString = input.get(0).toString();
        }
        if(input.size() == 2) {
            modelPath = input.get(1).toString();
            inputString = input.get(0).toString();
        }

        if(inputString == null || inputString == "") {
            return null;
        }

        DataBag outBag = bf.newDefaultBag();
        if(isFirst == true) {
            is = new FileInputStream(modelPath);
            model = new SentenceModel(is);
            sdetector = new SentenceDetectorME(model);

            isFirst = false;
        }
        String sentences[] = sdetector.sentDetect(inputString);
        for(String sentence : sentences) {
            Tuple outTuple = tf.newTuple(sentence);
            outBag.add(outTuple);
        }
        return outBag;
    }
}