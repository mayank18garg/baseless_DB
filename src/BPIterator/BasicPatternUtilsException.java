package BPIterator;
import chainexception.*;

public class BasicPatternUtilsException extends ChainException {
  public BasicPatternUtilsException(String s){super(null,s);}
  public BasicPatternUtilsException(Exception prev, String s){ super(prev,s);}
}
