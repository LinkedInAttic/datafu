// Code from https://github.com/benelog/multiline.git
// Based on Adrian Walker's blog post: http://www.adrianwalker.org/2011/12/java-multiline-string.html

package org.adrianwalker.multilinestring;

import java.util.Set;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;

import com.sun.tools.javac.model.JavacElements;
import com.sun.tools.javac.processing.JavacProcessingEnvironment;
import com.sun.tools.javac.tree.JCTree.JCVariableDecl;
import com.sun.tools.javac.tree.TreeMaker;

@SupportedAnnotationTypes({"org.adrianwalker.multilinestring.Multiline"})
@SupportedSourceVersion(SourceVersion.RELEASE_6)
public final class JavacMultilineProcessor extends AbstractProcessor {

  private JavacElements elementUtils;
  private TreeMaker maker;
  
  @Override
  public void init(final ProcessingEnvironment procEnv) {
    super.init(procEnv);
    JavacProcessingEnvironment javacProcessingEnv = (JavacProcessingEnvironment) procEnv;
    this.elementUtils = javacProcessingEnv.getElementUtils();
    this.maker = TreeMaker.instance(javacProcessingEnv.getContext());
  }

  @Override
  public boolean process(final Set<? extends TypeElement> annotations, final RoundEnvironment roundEnv) {
    Set<? extends Element> fields = roundEnv.getElementsAnnotatedWith(Multiline.class);
    for (Element field : fields) {
      String docComment = elementUtils.getDocComment(field);
      if (null != docComment) {
        JCVariableDecl fieldNode = (JCVariableDecl) elementUtils.getTree(field);
        fieldNode.init = maker.Literal(docComment);
      }
    }
    return true;
  }
}