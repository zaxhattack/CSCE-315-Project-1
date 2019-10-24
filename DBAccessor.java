import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;

public class DBAccessor {
    private Connection conn;

    //Constructor establishes the connection to the database
    public DBAccessor() {
        //Building the connection
        conn = null;
        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection("jdbc:postgresql://db-315.cse.tamu.edu/IMDb_Database", DBSetup.user, DBSetup.pswd);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(e.getClass().getName()+": "+e.getMessage());
            System.exit(1);
        }
        System.out.println("Opened database successfully");
    }


    //Takes an ArrayList of terms retrieved from the GUI and returns the ResultSet of the query
    public ResultSet requestMovies(ArrayList<SearchTerm> terms, String searchType){
        try{
            Statement sqlStmt = conn.createStatement();
            String stmtStr = "SELECT primarytitle, startyear, runtimeminutes, genres FROM title_basics WHERE ";

            //Checks each term and adds conditions based on the term type
            for(SearchTerm term: terms){
                switch(term.getType()){
                    case AND:
                        stmtStr += " AND ";
                        break;
                    case OR:
                        stmtStr += " OR ";
                        break;
                    case TITLE:
                        stmtStr += "primaryTitle = '" + term.getTerm() + "'";
                        break;
                    case RUNTIME:
                        stmtStr += "runtimeminutes = " + term.getTerm();
                        break;
                    case ACTOR:
                        String actorid = getNameIdOf(term.getTerm(), true);
                        stmtStr += "titleid IN (SELECT titleid FROM title_principals WHERE nameid = '" + actorid + "' AND (category = 'actor' OR category = 'actress'))";
                        break;
                    case DIRECTOR:
                        String directorid = getNameIdOf(term.getTerm(), false);
                        stmtStr += "titleid IN (SELECT titleid FROM title_principals WHERE nameid = '" + directorid + "' AND category = 'director')";
                        break;
                    case PRODUCER:
                        String producerid = getNameIdOf(term.getTerm(), false);
                        stmtStr += "titleid IN (SELECT titleid FROM title_principals WHERE nameid = '" + producerid + "' AND category = 'producer')";
                        break;
                    case YEAR:
                        stmtStr += "startyear = " + term.getTerm();
                        break;
                    default:
                        break;
                }
            }
            stmtStr += " AND titletype = '" + parseSearchType(searchType) + "'";
            System.out.println("requestMovies(): " + stmtStr);
            
            ResultSet result = sqlStmt.executeQuery(stmtStr);
            System.out.println("requestMovies(): Success");
            return result;
        }catch(Exception e){
            e.printStackTrace();
        }
        return null;
    }


    //Takes a director's name and returns a HashMap with the count of genres they've directed
    public HashMap<String, Integer> getDirectorsMovies(String director){
        String directorId = "";
        HashMap<String, Integer> genreCount = new HashMap<String, Integer>();
        
        try{
            Statement sqlStmt = conn.createStatement();
            //Use a customized version of getNameId() to try and get the most accurate person possible
            String stmtStr = "SELECT nameid FROM name_basics WHERE primaryname = '" + director + "' AND (primaryprofession LIKE '%director%' OR primaryprofession LIKE '%writer%')";
            
            ResultSet result = sqlStmt.executeQuery(stmtStr);
            //Assume that the first entry is the desired person
            //Presenting the user with all of the options wouldn't be ideal since we could have lots of identical looking entries
            if(result.next()){
                directorId = result.getString(1);
            }
            
            stmtStr = "SELECT genres FROM movie_basics WHERE titleid IN (SELECT titleid FROM movie_principals WHERE nameid = '" + directorId + "' AND (category = 'director' OR category = 'writer'))";
            
            result = sqlStmt.executeQuery(stmtStr);
            while(result.next()){
                for(String genre: result.getString(1).split(",")){
                    genreCount.put(genre, genreCount.getOrDefault(genre, 0) + 1);
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        
        return genreCount;
    }
    
    
    //Takes a SearchTerm with startYear and endYear, and returns the name of an actor who has performed in a movie in every year of that range
    public String getActorFromYears(SearchTerm yearsTerm, String excludeActorName){
        int startYear = yearsTerm.getStartYear();
        int endYear = yearsTerm.getEndYear();
        String excludeActorId = excludeActorName.equalsIgnoreCase("") ? "" : getNameIdOf(excludeActorName, true);
        
        try{
            // Create a query to get the list of possible actors
            Statement stmt = conn.createStatement();
            //Only select actors that were alive within the specified years
            String query = "SELECT nameid FROM name_basics_actors WHERE (birthyear <= " + startYear + " OR birthyear IS NULL) AND (deathyear >= " + endYear + " OR deathyear IS NULL)"; 
            
            if(!excludeActorId.equalsIgnoreCase("")){
                System.out.println("Exclude " + excludeActorName + ": " + excludeActorId);
                query += " AND nameid != '" + excludeActorId + "'";
            }
            
            ResultSet actorList = stmt.executeQuery(query);
            while(actorList.next()){
                //Must create a new Statement or the actorList ResultSet will close
                String currActorId = actorList.getString(1);
                Statement subStmt = conn.createStatement();
                //Select all the movies that currActorId has been in
                String subQuery = "SELECT mb.startyear FROM movie_principals mp JOIN movie_basics mb ON mp.nameid = '" + currActorId + "' AND mp.titleid = mb.titleid AND mb.startyear >= " + startYear + " AND mb.startyear <= " + endYear;
                
                //yesars[] is used to keep track of weather an actor has performed in a movie in a given year
                boolean[] years = new boolean[endYear-startYear+1];
                Arrays.fill(years, false);
                boolean success = true;
                
                ResultSet movieList = subStmt.executeQuery(subQuery);
                while(movieList.next()){
                    years[movieList.getInt(1)-startYear] = true;
                }
                
                System.out.println(currActorId);
                //Check to see if an actor has been in a movie for every single year in the specified range
                for(int i=0; i<endYear-startYear+1; i++){
                    if(!years[i]){
                        success = false;
                        break;
                    }
                }
                
                if(success){
                    System.out.println("Success: " + currActorId);
                    return getNameOf(currActorId);
                }else{
                    System.out.println("Failed: " + currActorId);
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        
        return "";
    }
    
    
    //Takes a SearchTerm that contians 2 actors and returns an ArrayList of String[], where each Stringp[] is a link between the actors
    //String[0] = actorid, String[1] = actor2id, String[2] = titleid
    public ArrayList<String[]> getLinkBetween(SearchTerm term, String excludeActorName){
        String actor1Id = getNameIdOf(term.getTerm(), true);
        String actor2Id = getNameIdOf(term.getTerm2(), true);
        String excludeActorId = excludeActorName.trim().equals("") ? "" : getNameIdOf(excludeActorName, true);
        System.out.println(term.getTerm() + ": " + actor1Id);
        System.out.println(term.getTerm2() + ": " + actor2Id);
        System.out.println("Exclude " + excludeActorName + ": " + excludeActorId);
        
        HashMap<String, String[]> visitedNodes = new HashMap<String, String[]>();
        LinkedList<String> toCheck = new LinkedList<String>();
        
        toCheck.add(actor1Id);
        visitedNodes.put(actor1Id, new String[]{"", ""});
        
        while(!toCheck.isEmpty()){
            String currActor = toCheck.removeFirst();
            System.out.println("Searching connections to: " + currActor);
            System.out.println("Goal: " + actor2Id);
            
            try{
                Statement stmt = conn.createStatement();
                //Select all of the actors that have a link to the current actor
                String query = "SELECT * FROM links WHERE nameid1 = '" + currActor + "' OR nameid2 = '" + currActor + "'";
                
                //If specified, exclude the specified actor from the chain
                if(!excludeActorId.equals("")){
                    query += " AND (nameid1 != '" + excludeActorId + "' AND nameid2 != '" + excludeActorId + "')";
                }
                
                ResultSet result = stmt.executeQuery(query);
                while(result.next()){
                    //Select the actor that isn't the one we're currnetly looking at
                    String connectedActorId = (result.getString(1).equalsIgnoreCase(currActor)) ? result.getString(2) : result.getString(1);
                    
                    //Add the newly connected actor to the queue so we can move forward in the graph
                    if(!visitedNodes.containsKey(connectedActorId)){
                        toCheck.add(connectedActorId);
                        visitedNodes.put(connectedActorId, new String[]{currActor, result.getString(3)});
                    }
                    
                    //Check to see if the newly connected actor is our endpoint
                    if(connectedActorId.equalsIgnoreCase(actor2Id)){
                        //tracePath() will return the path from the source to the destination
                        return tracePath(visitedNodes, actor1Id, connectedActorId);
                    }
                }
                
            }catch(Exception e){
                e.printStackTrace();
            }
        }
        
        System.out.println("No path found");
        return new ArrayList<String[]>();
    }
    
    
    /**
     * End of public facing methods. Start of private utility methods
     */
    
    
    //Takes a HashMap of the visitedNodes from getLinkBetween() and constructs the path from startId to endId   
    private ArrayList<String[]> tracePath(HashMap<String, String[]> connections, String startId, String endId){
        String currActor = endId;
        ArrayList<String[]> path = new ArrayList<String[]>();
        
        //Continue linking nodes untill we've reached the source
        while(!currActor.equalsIgnoreCase(startId)){
            String connectedActor = connections.get(currActor)[0];
            String connectedMovie = connections.get(currActor)[1];
            
            path.add(new String[]{ getNameOf(currActor), getNameOf(connectedActor), getTitleOf(connectedMovie, true) });
            
            currActor = connectedActor;
        }
        return path;
    }


    //Given a name, retrieve the nameid of the first matching person from name_basics
    private String getNameIdOf(String name, boolean actorsOnly){
        String tableName;
        //Give the option to limit the people to actors in order to speed up query times
        if(actorsOnly){
            tableName = "name_basics_actors";
        }else{
            tableName = "name_basics";
        }
        
        try{
            Statement sqlStmt = conn.createStatement();
            String stmtStr = "SELECT nameid FROM " + tableName + " WHERE primaryname = '" + name + "'";
            System.out.println("getNameIdOf(): " + stmtStr);
            ResultSet result = sqlStmt.executeQuery(stmtStr);
            System.out.println("getNameIdOf(): Success");
            //Assume that the first entry is the desired person
            //Presenting the user with all of the options wouldn't be ideal since we could have lots of identical looking entries
            if(result.next()){
                return result.getString(1);
            }
        }catch(Exception e){
            System.out.println("Error retrieving nameid of " + name);
            e.printStackTrace();
        }
        return "";
    }
    
    
    //Given a nameid, retrieve the person's name
    private String getNameOf(String nameid){
        try{
            Statement sqlStmt = conn.createStatement();
            String stmtStr = "SELECT primaryname FROM name_basics WHERE nameid = '" + nameid + "'";
            System.out.println("getNameOf(): " + stmtStr);
            ResultSet result = sqlStmt.executeQuery(stmtStr);
            System.out.println("getNameOf(): Success");
            if(result.next()){
                return result.getString(1);
            }
        }catch(Exception e){
            System.out.println("Error retrieving name of " + nameid);
            e.printStackTrace();
        }
        return "";
    }
    
    
    //Given a titleid, retrieve the title's name
    private String getTitleOf(String titleid, boolean moviesOnly){
        String tableName;
        //Give the option to limit the titles to movies in order to speed up query times
        if(moviesOnly){
            tableName = "movie_basics";
        }else{
            tableName = "title_basics";
        }
        
        try{
            Statement sqlStmt = conn.createStatement();
            String stmtStr = "SELECT primarytitle FROM " + tableName + " WHERE titleid = '" + titleid + "'";
            System.out.println("getTitleOf(): " + stmtStr);
            ResultSet result = sqlStmt.executeQuery(stmtStr);
            System.out.println("getTitleOf(): Success");
            //Assume that the first entry is the desired title
            //Presenting the user with all of the options wouldn't be ideal since we could have lots of identical looking entries
            if(result.next()){
                return result.getString(1);
            }
        }catch(Exception e){
            System.out.println("Error retrieving title of " + titleid);
            e.printStackTrace();
        }
        return "";
    }
    
    
    //Takes a string in the form on an enum keyword and returns the category
    private String parseSearchType(String searchType){
        switch(searchType){
            case "SHORT":
                return "short";
            case "TVSERIES":
                return "tvSeries";
            case "VIDEOGAME":
                return "videoGame";
            case "TVSPECIAL":
                return "tvSpecial";
            case "TVSHORT":
                return "tvShort";
            case "TVMOVIE":
                return "tvMovie";
            case "VIDEO":
                return "video";
            case "MOVIE":
                return "movie";
            case "TVMINISERIES":
                return "tvMiniSeries";
            default:
                return "";
        }
    }
}