/* Copyright (c) 2001-2018, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.hsqldb.types;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.hsqldb.jdbc.JDBCConnection;

/**
 * This class is responsible for providing User Defined Types (UDTs).
 *
 * @author Maciek Siemczyk (msiemczyk@live dot ca)
 * @version 1.0.0
 * @since 2.4.1
 */
public final class UserDefinedTypesProvider {

    /**
     * Local in-memory cache for used UDTs.
     */
    private final Set<String> typeNames = new HashSet<String>();
    
    /**
     * Connection to the database that we need to check UDT in.
     */
    private final JDBCConnection connection;
    
    /**
     * Default constructor.
     * 
     * @param connection Needed connection to the database.
     * 
     * @throws IllegalArgumentException if given connection is null.
     */
    public UserDefinedTypesProvider(final JDBCConnection connection) throws IllegalArgumentException {
        
        if (connection == null) {
            throw new IllegalArgumentException("Given connection is null.");
        }
        
        this.connection = connection;
    }

    public int getTypeNr(final String typeName) throws SQLException {

        if (typeNames.contains(typeName)) {
            return Types.SQL_UDT;
        }
        
        synchronized (typeNames) {
            if (typeNames.contains(typeName)) {
                return Types.SQL_UDT;
            }
            
            if (doesUdtExistInDatabase(typeName)) {
                typeNames.add(typeName);
                
                return Types.SQL_UDT;
            }
        }
        
        return Integer.MIN_VALUE;
    }
    
    private boolean doesUdtExistInDatabase(String typeName) throws SQLException {
        
        final String[] typeNameParts = typeName.split("\\.");
        
        String schemaName = null;
        
        if (typeNameParts.length > 1) {
            schemaName = typeNameParts[0];
            typeName = typeNameParts[1];
        }
        
        try (ResultSet resultSet = connection.getMetaData().getUDTs(null, schemaName, typeName, null)) {
            while (resultSet.next()) {
                final String userDefinedTypeName = resultSet.getString("TYPE_NAME");
                
                if (typeName.equals(userDefinedTypeName)) {
                    return true;
                }
            }
        }
        
        return false;
    }
}
