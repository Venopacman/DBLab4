package com.paul.database;


import java.sql.SQLException;
import java.util.Scanner;

/*
    Программа для работы с базой данных бюро занятости, с таблицами :
    employee[employee_id INTEGER PK, surname TEXT, employee_address TEXT, tax INTEGER]
    post[post_id INTEGER PK, post_name TEXT, hourly_pay INTEGER, max_hours INTEGER]
    job[job_id INTEGER PK, org_name TEXT, job_address TEXT, deduction INTEGER]
    work[work_number INTEGER PK, work_date TEXT, employee_id INTEGER FK, job_id INTEGER FK, post_id INTEGER FK,
    hours_numb INTEGER, payment INTEGER]
*/
public class Main {
    private static boolean exit = true;

    public static void main(String[] args) {
        DBController controller = new DBController();
        Scanner input = new Scanner(System.in);
        int menuNumber;
        String tableName;
        String customerName;
        int customerCredit;
        String prodName;
        while (exit) {
            System.out.println("Please enter the number of corresponding procedure you would like to make: \n" +
                    "1- Insert in table $Required table name$ \n" +
                    "2- Clear table $Required table name$ \n" +
                    "3- Show all info from table $Required table name$ \n" +
                    "4- Show info about one customer $Required customer name$ \n" +
                    "5- Update customer credit info $Required customer name & new credit value$ \n" +
                    "6- Delete information about product $Required product denomination(name)$ \n" +
                    "7- Exit and delete database \n");
            menuNumber = input.nextInt();
            switch (menuNumber) {
                //insert in some table
                case 1: {
                    System.out.println("Please text table name ex:[order,consumer,department,product]");
                    tableName = input.next();
                    try {
                        controller.insertInTable(tableName);
                    } catch (SQLException e) {
                        e.printStackTrace();
                        System.err.println("Incorrect table name!");
                    }
                    break;
                }
                //clear some table
                case 2: {
                    System.out.println("Please text table name ex:[order,consumer,department,product]");
                    tableName = input.next();
                    try {
                        controller.clearTable(tableName);
                    } catch (SQLException e) {
                        e.printStackTrace();
                        System.err.println("Incorrect table name!");
                    }
                    break;
                }
                //show info from some table
                case 3: {
                    System.out.println("Please text table name ex:[order,consumer,department,product]");
                    tableName = input.next();
                    try {
                        controller.showTable(tableName);
                    } catch (SQLException e) {
                        e.printStackTrace();
                        System.err.println("Incorrect table name!");
                    }
                    break;
                }
                //Search for one customer
                case 4: {
                    System.out.println("Please text customer name:");
                    customerName = input.next();
                    try {
                        controller.searchByConsumerName(customerName);
                    } catch (SQLException e) {
                        e.printStackTrace();
                        System.err.println("???????? ti cho nadelal???????????????");
                    }
                    break;
                }
                //Update customer  credit info table
                case 5: {
                    System.out.println("Please text customer name and new credit value ex:[name 5]");
                    customerName = input.next();
                    customerCredit = input.nextInt();
                    try {
                        controller.updateConsumerTable(customerCredit, customerName);
                    } catch (SQLException e) {
                        e.printStackTrace();
                        System.err.println("???????? ti cho nadelal???????????????");
                    }
                    break;
                }
                //Delete info from product table
                case 6: {
                    System.out.println("Please text product name:");
                    prodName = input.next();
                    try {
                        controller.deleteFromProductTable(prodName);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                    break;
                }
                //Exit
                case 7: {
                    System.err.println("You finish work with database " + controller.getDbName());
                    try {
                        controller.deleteDatabase(controller.getDbName());
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                    exit = false;
                    break;
                }
            }
        }
    }
}
