public class SearchTerm{
    public enum TermType{
        TITLE,
        ACTOR,
        PRODUCER,
        DIRECTOR,
        RUNTIME,
        YEAR,
        AND,
        OR
    }

    private String term;
    private String term2;
    private TermType termType;
    
    private int start_year, end_year;

    public SearchTerm(String st, String st2, TermType t, int styr, int endyr){
        term = st;
        term2 = st2;
        termType = t;
        start_year = styr;
        end_year = endyr;
    }

    public String getTerm(){
        return term;
    }
    
    public String getTerm2(){
        return term2;
    }

    public TermType getType(){
        return termType;
    }
    
    public int getStartYear(){
        return start_year;
    }
    
    public int getEndYear(){
        return end_year;
    }
}
