// Copyright (c) 2024 Boomi, LP.
package com.boomi.connector.hubspotcrm.util;

/**
 * Utility class for managing HubSpot CRM object associations.
 * <p>
 * The class maintains a predefined list of valid association types used in
 * HubSpot CRM operations and provides utility methods to work with these associations.
 */
public class ConstantAssociation {

    private static final String[] ASSOCIATIONS = {
            "contact", "company", "deal", "ticket", "call", "email", "meeting", "note", "task", "communication", "cart",
            "order", "invoice", "subscription" };

    // Private constructor to prevent instantiation
    private ConstantAssociation() {
    }

    /**
     * Returns a comma-separated string of valid associations.
     *
     * @return A String containing all valid associations joined by commas.
     * For example, if ASSOCIATIONS contains ["contact", "company", "deal"],
     * the method will return "contact,company,deal"
     */
    public static String getDefaultAssociations() {
        return String.join(ExecutionUtils.COMMA_SEPARATOR, ASSOCIATIONS);
    }
}
