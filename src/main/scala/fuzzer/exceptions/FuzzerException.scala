package fuzzer.exceptions

class FuzzerException(actualException: Throwable) extends Exception(actualException.getMessage)
