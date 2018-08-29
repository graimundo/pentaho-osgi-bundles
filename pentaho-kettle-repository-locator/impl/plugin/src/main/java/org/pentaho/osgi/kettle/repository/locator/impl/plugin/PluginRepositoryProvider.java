/*!
 * Copyright 2018 Hitachi Vantara.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.pentaho.osgi.kettle.repository.locator.impl.plugin;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.osgi.kettle.repository.locator.api.KettleRepositoryProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a kettle repository based on the information provided in the repositories.xml.
 * The repository is initialized and connected to the first time it is accessed.
 */
public final class PluginRepositoryProvider implements KettleRepositoryProvider {

  //region Properties
  private static final Logger logger = LoggerFactory.getLogger( PluginRepositoryProvider.class );

  public String getUsername() {
    return username;
  }
  public void setUsername( String repoUsername ) {
    this.username = repoUsername;
    this.shouldReconnect = true;
  }
  private String username;

  public String getPassword() {
    return password;
  }
  public void setPassword( String password) {
    this.password = password;
    this.shouldReconnect = true;
  }
  private String password;

  private boolean shouldReconnect = true;
  //endregion


  @Override
  public PurRepositoryProxy getRepository() {
    if( this.shouldReconnect) {
      this.reconnectToRepository();
    }
    return this.repository;

  }
  public void setRepository( PurRepositoryProxy repository ) {
    this.repository = repository;
  }
  private PurRepositoryProxy repository;

  /**
   * Resets the repository so that next time {@link #getRepository()} is called a new repository is returned.
   */
  public void reconnectToRepository() {
    if ( this.repository == null ) {
      return;
    }

    if( this.repository.isConnected() ) {
      this.repository.disconnect();
    }

    try {
      this.repository.connect( this.getUsername(), this.getPassword() );
      this.shouldReconnect = false;
    } catch ( KettleException e) {
      logger.debug( "Unable to connect to repository \"{}\".", this.repository.getRepositoryMeta().getId() );
    }
  }


}