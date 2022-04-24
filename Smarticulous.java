package smarticulous;

import smarticulous.db.Exercise;
import smarticulous.db.Submission;
import smarticulous.db.User;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * The Smarticulous class, implementing a grading system.
 */
public class Smarticulous {

    /**
     * The connection to the underlying DB.
     * <p>
     * null if the db has not yet been opened.
     */
    Connection db;

    /**
     * Open the {@link Smarticulous} SQLite database.
     * <p>
     * This should open the database, creating a new one if necessary, and set the {@link #db} field
     * to the new connection.
     * <p>
     * The open method should make sure the database contains the following tables, creating them if necessary:
     *
     * <table>
     *   <caption><em>Table name: <strong>User</strong></em></caption>
     *   <tr><th>Column</th><th>Type</th></tr>
     *   <tr><td>UserId</td><td>Integer (Primary Key)</td></tr>
     *   <tr><td>Username</td><td>Text</td></tr>
     *   <tr><td>Firstname</td><td>Text</td></tr>
     *   <tr><td>Lastname</td><td>Text</td></tr>
     *   <tr><td>Password</td><td>Text</td></tr>
     * </table>
     *
     * <p>
     * <table>
     *   <caption><em>Table name: <strong>Exercise</strong></em></caption>
     *   <tr><th>Column</th><th>Type</th></tr>
     *   <tr><td>ExerciseId</td><td>Integer (Primary Key)</td></tr>
     *   <tr><td>Name</td><td>Text</td></tr>
     *   <tr><td>DueDate</td><td>Integer</td></tr>
     * </table>
     *
     * <p>
     * <table>
     *   <caption><em>Table name: <strong>Question</strong></em></caption>
     *   <tr><th>Column</th><th>Type</th></tr>
     *   <tr><td>ExerciseId</td><td>Integer</td></tr>
     *   <tr><td>QuestionId</td><td>Integer</td></tr>
     *   <tr><td>Name</td><td>Text</td></tr>
     *   <tr><td>Desc</td><td>Text</td></tr>
     *   <tr><td>Points</td><td>Integer</td></tr>
     * </table>
     * In this table the combination of ExerciseId and QuestionId together comprise the primary key.
     *
     * <p>
     * <table>
     *   <caption><em>Table name: <strong>Submission</strong></em></caption>
     *   <tr><th>Column</th><th>Type</th></tr>
     *   <tr><td>SubmissionId</td><td>Integer (Primary Key)</td></tr>
     *   <tr><td>UserId</td><td>Integer</td></tr>
     *   <tr><td>ExerciseId</td><td>Integer</td></tr>
     *   <tr><td>SubmissionTime</td><td>Integer</td></tr>
     * </table>
     *
     * <p>
     * <table>
     *   <caption><em>Table name: <strong>QuestionGrade</strong></em></caption>
     *   <tr><th>Column</th><th>Type</th></tr>
     *   <tr><td>SubmissionId</td><td>Integer</td></tr>
     *   <tr><td>QuestionId</td><td>Integer</td></tr>
     *   <tr><td>Grade</td><td>Real</td></tr>
     * </table>
     * In this table the combination of SubmissionId and QuestionId together comprise the primary key.
     *
     * @param dburl The JDBC url of the database to open (will be of the form "jdbc:sqlite:...")
     * @return the new connection
     * @throws SQLException
     */
    public Connection openDB(String dburl) throws SQLException {
        db = DriverManager.getConnection(dburl); //connecting to drivermanager
        Statement st = db.createStatement(); //st is our statement to be execute on db
        //we want to update or add tables to database so we use executeUpdate
        st.executeUpdate("CREATE TABLE IF NOT EXISTS User (UserId INTEGER PRIMARY KEY, Username TEXT UNIQUE," +
                "Firstname TEXT, Lastname TEXT, Password TEXT)");
        st.executeUpdate("CREATE TABLE IF NOT EXISTS Exercise(ExerciseId INTEGER PRIMARY KEY, Name TEXT, DueDate INTEGER)");
        st.executeUpdate("CREATE TABLE IF NOT EXISTS Question(ExerciseId INTEGER, QuestionId INTEGER, Name TEXT, " +
                "Desc TEXT, Points INTEGER, PRIMARY KEY(ExerciseId,QuestionId))");
        st.executeUpdate("CREATE TABLE IF NOT EXISTS Submission(SubmissionId INTEGER PRIMARY KEY, UserId INTEGER," +
                "ExerciseId INTEGER, SubmissionTime INTEGER)");
        st.executeUpdate("CREATE TABLE IF NOT EXISTS QuestionGrade(SubmissionId INTEGER,QuestionId INTEGER, Grade" +
                " Real, PRIMARY KEY(SubmissionId,QuestionId))");
        return db;
    }


    /**
     * Close the DB if it is open.
     *
     * @throws SQLException
     */
    public void closeDB() throws SQLException {
        if (db != null) {
            db.close();
            db = null;
        }
    }

    // =========== User Management =============

    /**
     * Add a user to the database / modify an existing user.
     * <p>
     * Add the user to the database if they don't exist. If a user with user.username does exist,
     * update their password and firstname/lastname in the database.
     *
     * @param user
     * @param password
     * @return the userid.
     * @throws SQLException
     */
    public int addOrUpdateUser(User user, String password) throws SQLException {
        String query = "SELECT * FROM User WHERE Username= ?;";// our statement to be executed
        PreparedStatement ps = db.prepareStatement(query);
        ps.setString(1,user.username);//want to catch the row where the username=user.username(if any)
        ResultSet res = ps.executeQuery();//save the row from the table in res
        Statement st1 = db.createStatement();//no need to use prepared statement here so we are using statement
        ResultSet id = st1.executeQuery("SELECT MAX(UserId) FROM User;");//catch the maximum num of userid
        int newId = id.getInt("MAX(UserId)") +1;//the id of the added user will be 1+ the max userid from the table
        if(res.next()){ // check if the user does exist so only need to update
            ps = db.prepareStatement("UPDATE User SET Password= ?, Firstname=?, Lastname=? WHERE Username= ? ;");
            ps.setString(1,password);
            ps.setString(2, user.firstname);
            ps.setString(3, user.lastname);
            ps.setString(4, user.username);
            ps.executeUpdate();
            newId=res.getInt("UserId");
            ps.close();

        }
        else { //need to add the user to table
            String query1 = "INSERT into User(UserId,Username,Firstname,Lastname,Password) VALUES (?,?,?,?,?);";
            ps = db.prepareStatement(query1);;
            ps.setInt(1,newId);
            ps.setString(2, user.username);
            ps.setString(3, user.firstname);
            ps.setString(4, user.lastname);
            ps.setString(5,password);
            ps.executeUpdate();
            ps.close();

        }
        return (newId);
    }


    /**
     * Verify a user's login credentials.
     *
     * @param username
     * @param password
     * @return true if the user exists in the database and the password matches; false otherwise.
     * @throws SQLException
     * <p>
     * Note: this is totally insecure. For real-life password checking, it's important to store only
     * a password hash
     * @see <a href="https://crackstation.net/hashing-security.htm">How to Hash Passwords Properly</a>
     */
    public boolean verifyLogin(String username, String password) throws SQLException {
        String query = "SELECT Username,Password FROM User WHERE Username=? and Password=?;";//our sql statement
        PreparedStatement myStmt = db.prepareStatement(query);
        myStmt.setString(1,username);//user.username=username
        myStmt.setString(2,password);//user.password=password
        ResultSet myRes = myStmt.executeQuery();//saving the answer in myRes
        return myRes.next();//if the user is exists muRes=true , else false
    }

    // =========== Exercise Management =============

    /**
     * Add an exercise to the database.
     *
     * @param exercise
     * @return the new exercise id, or -1 if an exercise with this id already existed in the database.
     * @throws SQLException
     */
    public int addExercise(Exercise exercise) throws SQLException {
        String query = "SELECT * FROM Exercise WHERE ExerciseId=?"; //want to catch the row which ExeciseId=exercise.id
        PreparedStatement myStmtEx = db.prepareStatement(query);
        PreparedStatement myStmtQus = null;
        myStmtEx.setInt(1, exercise.id);
        ResultSet res = myStmtEx.executeQuery();
        if(res.next()){ //if the exercise already exists
            return -1;
        } //first we are going to add the new exercise to the Question table. then to the Exercise table
        else {
            String query1 = "INSERT into Question(ExerciseId,Name,Desc,Points) VALUES(?,?,?,?);";//we add a new exercise to table Question
            myStmtQus = db.prepareStatement(query1);
            for(int i = 0; i<exercise.questions.size(); i++){//in each exercise the is a list of question so we use a loop to add al relevant questions
                myStmtQus.setInt(1,exercise.id);
                myStmtQus.setString(2,exercise.questions.get(i).name);
                myStmtQus.setString(3,exercise.questions.get(i).desc);
                myStmtQus.setInt(4,exercise.questions.get(i).points);
                myStmtQus.executeUpdate();
            }


            String query2 = "INSERT into Exercise(ExerciseId,Name,DueDate) VALUES(?,?,?);";//we add a new exercise to table Exercise
            myStmtEx = db.prepareStatement(query2);
            myStmtEx.setInt(1,exercise.id);
            myStmtEx.setString(2,exercise.name);
            myStmtEx.setLong(3,exercise.dueDate.getTime());
            myStmtEx.executeUpdate();
            int idToReturn = exercise.id;
            return (idToReturn);

        }

    }


    /**
     * Return a list of all the exercises in the database.
     * <p>
     * The list should be sorted by exercise id.
     *
     * @return list of all exercises.
     * @throws SQLException
     */
    public List<Exercise> loadExercises() throws SQLException {
        String query = "SELECT * FROM Exercise ORDER BY ExerciseId;";//catch all the Exercise table ordered by its id
        PreparedStatement st = db.prepareStatement(query);
        ResultSet res = st.executeQuery();
        ArrayList<Exercise> ex = new ArrayList<>(); //array list to be returned
        while(res.next()){
            Exercise newEx = new Exercise(res.getInt("ExerciseId"),res.getString("Name") //a new exercise object
                    ,res.getDate("DueDate"));
            String queryForQue = "SELECT * FROM Question WHERE ExerciseId=?;";
            PreparedStatement st1 = db.prepareStatement(queryForQue);
            st1.setInt(1,newEx.id);
            ResultSet res1 = st1.executeQuery();
            while (res1.next()){
                newEx.addQuestion(res1.getString("Name"),(res1.getString("Desc")), //adding to each exercise its question list
                        (res1.getInt("Points")));

            }
            ex.add(newEx); //adding the exercise to the array list that we created
        }
        return ex; //returning the lst
    }

    // ========== Submission Storage ===============

    /**
     * Store a submission in the database.
     * The id field of the submission will be ignored if it is -1.
     * <p>
     * Return -1 if the corresponding user doesn't exist in the database.
     *
     * @param submission
     * @return the submission id.
     * @throws SQLException
     */
    public int storeSubmission(Submission submission) throws SQLException {
        int id = submission.id;
        PreparedStatement st = null;
        PreparedStatement st1 = null;
        ResultSet res = null;
        ResultSet res1 = null;
        String query1 = "";
        String query = "";
        //each submission includes details about the exercise and the user.so we need to create two objects one exercise and one for the user
        Exercise subEx = new Exercise(submission.exercise.id,submission.exercise.name,submission.exercise.dueDate);
        User subUser = new User(submission.user.username,submission.user.firstname,submission.user.lastname);
        query = "SELECT UserId FROM User WHERE Username=?;"; //sql statement for getting the user id to check if it -1
        st = db.prepareStatement(query);
        st.setString(1,subUser.username);
        res = st.executeQuery();
        if(res.next()) {
            if (id == -1) { //ignoring the submission id
                query1 = "INSERT INTO Submission(UserId,ExerciseId,SubmissionTime) VALUES (?,?,?);"; //add the new submission
                query = "SELECT SubmissionId FROM Submission WHERE ExerciseId=? AND SubmissionTime=? AND UserId=?;";
                st1 = db.prepareStatement(query1);
                st1.setInt(1, res.getInt("UserId"));
                st1.setInt(2, subEx.id);
                st1.setLong(3, submission.submissionTime.getTime());
                st1.executeUpdate();
                st = db.prepareStatement(query);
                st.setInt(1,subEx.id);
                st.setLong(2,submission.submissionTime.getTime());
                st.setInt(3,res.getInt("UserId"));
                res1 = st.executeQuery();
                return (res1.getInt("SubmissionId"));
            } else { //the submission id is not -1
                query1 = "INSERT INTO Submission(SubmissionId,UserId,ExerciseId,SubmissionTime) VALUES (?,?,?,?);";
                st1 = db.prepareStatement(query1);
                st1.setInt(1, id);
                st1.setInt(2, res.getInt("UserId"));
                st1.setInt(3, subEx.id);
                st1.setLong(4, submission.submissionTime.getTime());
                st1.executeUpdate();
                return (id);
            }
        }
        return -1;
    }


    // ============= Submission Query ===============


    /**
     * Return a prepared SQL statement that, when executed, will
     * return one row for every question of the latest submission for the given exercise by the given user.
     * <p>
     * The rows should be sorted by QuestionId, and each row should contain:
     * - A column named "SubmissionId" with the submission id.
     * - A column named "QuestionId" with the question id,
     * - A column named "Grade" with the grade for that question.
     * - A column named "SubmissionTime" with the time of submission.
     * <p>
     * Parameter 1 of the prepared statement will be set to the User's username, Parameter 2 to the Exercise Id, and
     * Parameter 3 to the number of questions in the given exercise.
     * <p>
     * This will be used by {@link #getLastSubmission(User, Exercise)}
     *
     * @return
     */
    PreparedStatement getLastSubmissionGradesStatement() throws SQLException {
       PreparedStatement st = db.prepareStatement("SELECT Submission.SubmissionId,QuestionId,Grade,SubmissionTime FROM " +
               "Submission INNER JOIN QuestionGrade ON Submission.SubmissionId=QuestionGrade.SubmissionId " +
               "INNER JOIN User ON Submission.UserId=User.UserId " +
               "WHERE User.Username=? AND ExerciseId=? " +
               "ORDER BY SubmissionTime DESC,QuestionId LIMIT ?;");
       //we need to join 3 tables to get all the 4 column requested and join them by the keys:submission id and user id.
        //the table adjusted to the given user and exercise.(by the question marks)
        //eventually the table will be sorted by question id ,and the numbers af rows that will
        // displayed is as the number of questions appropriate to the exercise(by LIMIT)
        return st;
    }

    /**
     * Return a prepared SQL statement that, when executed, will
     * return one row for every question of the <i>best</i> submission for the given exercise by the given user.
     * The best submission is the one whose point total is maximal.
     * <p>
     * The rows should be sorted by QuestionId, and each row should contain:
     * - A column named "SubmissionId" with the submission id.
     * - A column named "QuestionId" with the question id,
     * - A column named "Grade" with the grade for that question.
     * - A column named "SubmissionTime" with the time of submission.
     * <p>
     * Parameter 1 of the prepared statement will be set to the User's username, Parameter 2 to the Exercise Id, and
     * Parameter 3 to the number of questions in the given exercise.
     * <p>
     * This will be used by {@link #getBestSubmission(User, Exercise)}
     *
     */
    PreparedStatement getBestSubmissionGradesStatement() throws SQLException {
        // TODO: Implement
        return null;
    }

    /**
     * Return a submission for the given exercise by the given user that satisfies
     * some condition (as defined by an SQL prepared statement).
     * <p>
     * The prepared statement should accept the user name as parameter 1, the exercise id as parameter 2 and a limit on the
     * number of rows returned as parameter 3, and return a row for each question corresponding to the submission, sorted by questionId.
     * <p>
     * Return null if the user has not submitted the exercise (or is not in the database).
     *
     * @param user
     * @param exercise
     * @param stmt
     * @return
     * @throws SQLException
     */
    Submission getSubmission(User user, Exercise exercise, PreparedStatement stmt) throws SQLException {
        stmt.setString(1, user.username);
        stmt.setInt(2, exercise.id);
        stmt.setInt(3, exercise.questions.size());

        ResultSet res = stmt.executeQuery();

        boolean hasNext = res.next();
        if (!hasNext)
            return null;

        int sid = res.getInt("SubmissionId");
        Date submissionTime = new Date(res.getLong("SubmissionTime"));

        float[] grades = new float[exercise.questions.size()];

        for (int i = 0; hasNext; ++i, hasNext = res.next()) {
            grades[i] = res.getFloat("Grade");
        }

        return new Submission(sid, user, exercise, submissionTime, (float[]) grades);
    }

    /**
     * Return the latest submission for the given exercise by the given user.
     * <p>
     * Return null if the user has not submitted the exercise (or is not in the database).
     *
     * @param user
     * @param exercise
     * @return
     * @throws SQLException
     */
    public Submission getLastSubmission(User user, Exercise exercise) throws SQLException {
        return getSubmission(user, exercise, getLastSubmissionGradesStatement());
    }


    /**
     * Return the submission with the highest total grade
     *
     * @param user the user for which we retrieve the best submission
     * @param exercise the exercise for which we retrieve the best submission
     * @return
     * @throws SQLException
     */
    public Submission getBestSubmission(User user, Exercise exercise) throws SQLException {
        return getSubmission(user, exercise, getBestSubmissionGradesStatement());
    }
}
