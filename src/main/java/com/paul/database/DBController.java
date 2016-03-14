package com.paul.database;


import java.sql.*;
import java.util.Scanner;

/**
 * Created by Paul on 13.03.2016.
 */
public class DBController {
    private Connection db;
    private String login;
    private String password;
    private String url;

    public String getDbName() {
        return dbName;
    }

    private String dbName;

    DBController() {

        try {
            Class.forName("org.postgresql.Driver");
            System.out.println("Driver connected!");
            connect();
            System.out.println("Connection established!");
            createDatabase();
            System.out.println("Database created!");
            connect(this.dbName);
            System.out.println("Connection with Database established!");
            initFunctions();
            System.out.println("Functions initialized!");
            createTables();
            System.out.println("Tables created!");
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    void connect() throws SQLException {
        if (url == null) {
            Scanner input = new Scanner(System.in);
            System.out.print("Please enter your url: ");
            url = input.nextLine();
            if (url.equals("")) url = "jdbc:postgresql://localhost:5432/";
            System.out.print("Please enter your login: ");
            login = input.nextLine();
            if (login.equals("")) login = "postgres";
            System.out.print("Please enter your password: ");
            password = input.nextLine();
            if (password.equals("")) password = "postgres";
            db = DriverManager.getConnection(url, login, password);
        } else
            url = "jdbc:postgresql://localhost:5432/";
        db = DriverManager.getConnection(url, this.login, this.password);
    }

    void connect(String dbName) throws SQLException {
        url = "jdbc:postgresql://localhost:5432/" + dbName;
        db = DriverManager.getConnection(url, this.login, this.password);
    }

    void createDatabase() throws SQLException {
        Scanner input = new Scanner(System.in);
        System.out.println("Please enter your DB name: ");
        this.dbName = input.next();
        CallableStatement callableStatement = db.prepareCall("{call create_db(?)}");
        callableStatement.setString(1, this.dbName);
        callableStatement.executeUpdate();
    }

    void deleteDatabase(String name) throws SQLException {
        db.close();
        connect();
        CallableStatement callableStatement = db.prepareCall("{call delete_db(?)}");
        callableStatement.setString(1, this.dbName);
        callableStatement.executeUpdate();
    }

    void createTables() throws SQLException {
        CallableStatement callableStatement = db.prepareCall("{call createTables()}");
        callableStatement.executeUpdate();
    }

    void initFunctions() throws SQLException {
        Statement statement;
        statement = db.createStatement();

        statement.executeUpdate("CREATE OR REPLACE FUNCTION create_db(db_name VARCHAR)\n" +
                "RETURNS VOID AS\n" +
                "$func$\n" +
                "BEGIN \n" +
                "PERFORM dblink_connect('dbname='|| current_database() ||' port=5432 host=localhost user=postgres password=postgres');\n" +
                "PERFORM dblink_exec('CREATE DATABASE ' || quote_ident(db_name));\n" +
                "END;\n" +
                "$func$\n" +
                "LANGUAGE plpgsql;"
        );

        statement.executeUpdate("CREATE OR REPLACE FUNCTION createTables() RETURNS VOID \n" +
                "AS $$\n" +
                "BEGIN\n" +
                "CREATE TABLE consumer (\"Id\" integer NOT NULL, \n" +
                "name VARCHAR, \n" +
                "\"Address\" VARCHAR, \n" +
                "credit integer);\n" +
                "CREATE TABLE department(\"Id\" integer NOT NULL, \n" +
                "\"Second_Name_of_Head\" VARCHAR, \n" +
                "\"Address\" VARCHAR, \n" +
                "\"Commission\" integer);\n" +
                "CREATE TABLE \"order\" ( \"number\" integer,\n" +
                "date VARCHAR,\n" +
                "depart integer,\n" +
                "consumer integer,\n" +
                "product integer,\n" +
                "amount integer,\n" +
                "\"SUM\" integer);\n" +
                "CREATE TABLE product (\"Id\" integer,\n" +
                "denomination VARCHAR,\n" +
                "price double precision,\n" +
                "storage VARCHAR,\n" +
                "max_order integer );\n" +
                "END;\n" +
                "$$ LANGUAGE plpgsql;"
        );

        statement.executeUpdate("CREATE OR REPLACE FUNCTION delete_db(db_name VARCHAR)\n" +
                "RETURNS VOID AS\n" +
                "$func$\n" +
                "BEGIN \n" +
                "PERFORM dblink_connect('dbname='|| current_database() ||' port=5432 host=localhost user=postgres password=postgres');\n" +
                "PERFORM dblink_exec('DROP DATABASE ' || quote_ident(db_name));\n" +
                "END;\n" +
                "$func$\n" +
                "LANGUAGE plpgsql;"
        );


        statement.executeUpdate("CREATE OR REPLACE FUNCTION update_consumer(\"Id\" INTEGER , name VARCHAR(30), \"Address\" VARCHAR(30), credit double precision)\n" +
                "RETURNS VOID AS $$\n" +
                "BEGIN \n" +
                "INSERT INTO consumer VALUES (\"Id\",name,\"Address\",credit);\n" +
                "END;$$\n" +
                "LANGUAGE plpgsql;"
        );

        statement.executeUpdate("CREATE OR REPLACE FUNCTION update_department(\"Id\" INTEGER, \"Second_Name_of_Head\" VARCHAR(30), \"Address\" VARCHAR(30), \"Commission\" double precision)\n" +
                "RETURNS VOID AS $$\n" +
                "BEGIN\n" +
                "INSERT INTO department VALUES (\"Id\",\"Second_Name_of_Head\",\"Address\",\"Commission\");\n" +
                "END;$$ \n" +
                "LANGUAGE plpgsql;"
        );

        statement.executeUpdate("CREATE OR REPLACE FUNCTION update_order(\"number\" integer ,\"date\" varchar(30), depart integer, consumer integer, \n" +
                " product integer,  amount integer, \"SUM\" integer)\n" +
                "RETURNS VOID AS $$\n" +
                "BEGIN\n" +
                "INSERT INTO \"order\" VALUES (\"number\",\"date\",depart,consumer,product,amount,\"SUM\");\n" +
                "END;\n" +
                "$$ \n" +
                "LANGUAGE plpgsql;"
        );

        statement.executeUpdate("CREATE OR REPLACE FUNCTION update_product(\"Id\" INTEGER, denomination VARCHAR(30) , price double precision, storage VARCHAR(30), max_order INTEGER)\n" +
                "RETURNS VOID AS $$\n" +
                "BEGIN\n" +
                "INSERT INTO product VALUES (\"Id\",denomination,price,storage,max_order);\n" +
                "END;$$ \n" +
                "LANGUAGE plpgsql;"
        );

        statement.executeUpdate("CREATE OR REPLACE FUNCTION clearTable(tableName VARCHAR) \n" +
                "RETURNS VOID \n" +
                "AS $func$\n" +
                "BEGIN\n" +
                "DELETE FROM tableName;\n" +
                "END;\n" +
                "$func$ LANGUAGE plpgsql;"
        );

        statement.executeUpdate("CREATE OR REPLACE FUNCTION tableSearchFromConsumer(consumerName VARCHAR)\n" +
                "RETURNS TABLE (order_id INTEGER ,date VARCHAR,nameOfConsumer VARCHAR, \n" +
                "consumerAddress VARCHAR, secondNameOfDepartmentHead VARCHAR, \n" +
                "departmentAddress VARCHAR,productName VARCHAR,productPrice double precision, \n" +
                "productAmount INTEGER, sum INTEGER ) as $BODY$\n" +
                "BEGIN\n" +
                "RETURN QUERY\n" +
                "Select o.number,o.date,c.name,c.\"Address\",d.\"Second_Name_of_Head\",d.\"Address\",p.denomination,p.price, o.amount, o.\"SUM\" from \n" +
                "\"order\" o join consumer c on (o.consumer=c.\"Id\") join department d on (o.depart=d.\"Id\") join product p on (o.product=p.\"Id\") \n" +
                "where c.name= $1;\n" +
                "END;\n" +
                "$BODY$ LANGUAGE plpgsql;"
        );

        statement.executeUpdate("CREATE OR REPLACE FUNCTION showOrder()\n" +
                "RETURNS TABLE (\"number\" integer,\n" +
                "  date VARCHAR,\n" +
                "  depart integer,\n" +
                "  consumer integer,\n" +
                "  product integer,\n" +
                "  amount integer,\n" +
                "  \"SUM\" integer ) as $BODY$\n" +
                "BEGIN\n" +
                "RETURN QUERY\n" +
                "Select * from \"order\";\n" +
                "END;\n" +
                "$BODY$ LANGUAGE plpgsql;"
        );

        statement.executeUpdate("CREATE OR REPLACE FUNCTION showConsumer()\n" +
                "RETURNS TABLE (\"Id\" integer,\n" +
                "  name VARCHAR,\n" +
                "  \"Address\" VARCHAR,\n" +
                "  credit integer ) as $BODY$\n" +
                "BEGIN\n" +
                "RETURN QUERY\n" +
                "Select * from consumer;\n" +
                "END;\n" +
                "$BODY$ LANGUAGE plpgsql;"
        );

        statement.executeUpdate("CREATE OR REPLACE FUNCTION showDepartment()\n" +
                "RETURNS TABLE (\"Id\" integer ,\n" +
                "  \"Second_Name_of_Head\" VARCHAR,\n" +
                "  \"Address\" VARCHAR,\n" +
                "  \"Commission\" integer ) as $BODY$\n" +
                "BEGIN\n" +
                "RETURN QUERY\n" +
                "Select * from department;\n" +
                "END;\n" +
                "$BODY$ LANGUAGE plpgsql;"
        );

        statement.executeUpdate("CREATE OR REPLACE FUNCTION showProduct()\n" +
                "RETURNS TABLE (\"Id\" integer,\n" +
                "  denomination VARCHAR,\n" +
                "  price double precision,\n" +
                "  storage VARCHAR,\n" +
                "  max_order integer ) as $BODY$\n" +
                "BEGIN\n" +
                "RETURN QUERY\n" +
                "Select * from product;\n" +
                "END;\n" +
                "$BODY$ LANGUAGE plpgsql;"
        );

        statement.executeUpdate("CREATE OR REPLACE FUNCTION updateConsumeCredit(newCredit INTEGER, updConsumerName VARCHAR)\n" +
                "RETURNS VOID \n" +
                "as $BODY$\n" +
                "BEGIN\n" +
                "UPDATE consumer SET credit = newCredit WHERE name = updConsumerName;\n" +
                "END;\n" +
                "$BODY$ LANGUAGE plpgsql;"
        );

        statement.executeUpdate("CREATE OR REPLACE FUNCTION deleteFromProduct(productName VARCHAR)\n" +
                "RETURNS VOID \n" +
                "AS $BODY$\n" +
                "BEGIN\n" +
                "DELETE FROM product\n" +
                "where denomination = $1;\n" +
                "END;\n" +
                "$BODY$ LANGUAGE plpgsql;"
        );
    }

    void insertInTable(String tableName) throws SQLException {
        Scanner input = new Scanner(System.in);
        switch (tableName) {
            case "consumer": {
                int id;
                String name;
                String address;
                double credit;
                CallableStatement callableStatement = db.prepareCall("{call update_consumer(?,?,?,?)}");
                System.out.println("Please input consumer data like [id name address credit]:");
                id = input.nextInt();
                name = input.next();
                address = input.next();
                credit = Double.valueOf(input.next());
                callableStatement.setInt(1, id);
                callableStatement.setString(2, name);
                callableStatement.setString(3, address);
                callableStatement.setDouble(4, credit);
                callableStatement.executeUpdate();
                break;
            }
            case "department": {
                int id;
                String secondName;
                String address;
                double commission;
                CallableStatement callableStatement = db.prepareCall("{call update_department(?,?,?,?)}");
                System.out.println("Please input department data like [id secondName address commission]:");
                id = input.nextInt();
                secondName = input.next();
                address = input.next();
                commission = input.nextDouble();
                callableStatement.setInt(1, id);
                callableStatement.setString(2, secondName);
                callableStatement.setString(3, address);
                callableStatement.setDouble(4, commission);
                callableStatement.executeUpdate();
                break;
            }
            case "order": {
                int number;
                String date;
                int depart;
                int consumer;
                int product;
                int amount;
                int sum;
                CallableStatement callableStatement = db.prepareCall("{call update_order(?,?,?,?,?,?,?)}");
                System.out.println("Please input order data like [number yyyy-mm-dd depart_id consumer_idb product_id amount sum]:");
                number = input.nextInt();
                date = input.next();
                depart = input.nextInt();
                consumer = input.nextInt();
                product = input.nextInt();
                amount = input.nextInt();
                sum = input.nextInt();
                callableStatement.setInt(1, number);
                callableStatement.setString(2, date);
                callableStatement.setInt(3, depart);
                callableStatement.setInt(4, consumer);
                callableStatement.setInt(5, product);
                callableStatement.setInt(6, amount);
                callableStatement.setInt(7, sum);
                callableStatement.executeUpdate();
                break;
            }
            case "product": {
                int id;
                String denomination;
                double price;
                String storage;
                int max_order;
                CallableStatement callableStatement = db.prepareCall("{call update_product(?,?,?,?,?)}");
                System.out.println("Please input product data like [id denomination price storage max_order]:");
                id = input.nextInt();
                denomination = input.next();
                price = input.nextDouble();
                storage = input.next();
                max_order = input.nextInt();
                callableStatement.setInt(1, id);
                callableStatement.setString(2, denomination);
                callableStatement.setDouble(3, price);
                callableStatement.setString(4, storage);
                callableStatement.setInt(5, max_order);
                callableStatement.executeUpdate();
                break;
            }
        }
    }

    void clearTable(String tableName) throws SQLException {
        CallableStatement callableStatement = db.prepareCall("{call clearTable(?)}");
        callableStatement.setString(1, tableName);
        callableStatement.executeUpdate();
    }

    void showTable(String tableName) throws SQLException {
        switch (tableName) {
            case "order": {
                CallableStatement callableStatement = db.prepareCall("{call showOrder()}");
                callableStatement.executeQuery();
            }
            case "consumer": {
                CallableStatement callableStatement = db.prepareCall("{call showConsumer()}");
                callableStatement.executeQuery();
            }
            case "department": {
                CallableStatement callableStatement = db.prepareCall("{call showDepartment()}");
                callableStatement.executeQuery();
            }
            case "product": {
                CallableStatement callableStatement = db.prepareCall("{call showProduct()}");
                callableStatement.executeQuery();
            }
        }

    }

    void searchByConsumerName(String consumerName) throws SQLException {
        CallableStatement callableStatement = db.prepareCall("{call tableSearchFromConsumer(?)}");
        callableStatement.setString(1, consumerName);
        callableStatement.executeQuery();
    }

    void updateConsumerTable(int newCredit, String updConsumerName) throws SQLException {
        CallableStatement callableStatement = db.prepareCall("{call updateConsumeCredit(?,?)}");
        callableStatement.setInt(1, newCredit);
        callableStatement.setString(2, updConsumerName);
        callableStatement.executeQuery();
    }

    void deleteFromProductTable(String prodName) throws SQLException {
        CallableStatement callableStatement = db.prepareCall("{call deleteFromProduct(?)}");
        callableStatement.setString(1, prodName);
        callableStatement.executeQuery();
    }
}
