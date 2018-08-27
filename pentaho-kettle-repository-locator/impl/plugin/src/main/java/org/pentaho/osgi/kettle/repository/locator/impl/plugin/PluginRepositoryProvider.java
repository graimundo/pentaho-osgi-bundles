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
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.RepositoryPluginType;
import org.pentaho.di.repository.RepositoriesMeta;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryMeta;
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

  public String getRepoName() {
    return repoName;
  }
  public void setRepoName( String repoName ) {
    this.repoName = repoName;
    this.resetRepository();
  }
  private String repoName;

  public String getRepoUsername() {
    return repoUsername;
  }
  public void setRepoUsername( String repoUsername ) {
    this.repoUsername = repoUsername;
    this.resetRepository();
  }
  private String repoUsername;

  public String getRepoPassword() {
    return repoPassword;
  }
  public void setRepoPassword( String repoPassword ) {
    this.repoPassword = repoPassword;
    this.resetRepository();
  }
  private String repoPassword;

  /**
   * @return the available repositories information
   */
  public RepositoriesMeta getRepositoriesMeta() {
    return repositoriesMeta;
  }
  public void setRepositoriesMeta( RepositoriesMeta repositoriesMeta ) {
    this.repositoriesMeta = repositoriesMeta;
    this.resetRepository();
  }
  private RepositoriesMeta repositoriesMeta;

  /**
   * @return the plugin registry used for loading the repository class
   */
  public PluginRegistry getPluginRegistry() {
    return this.pluginRegistry;
  }
  public void setPluginRegistry( PluginRegistry pluginRegistry ) {
    this.pluginRegistry = pluginRegistry;
    this.resetRepository();
  }
  private PluginRegistry pluginRegistry;
  //endregion

  /**
   * Reads the repository information from repositories.xml.
   * @return <code>null</code> if the repository is not found.
   */
  private RepositoryMeta readRepositoryMeta( String repoName ) {
    RepositoriesMeta repositoriesMeta = this.getRepositoriesMeta();

    try {
      repositoriesMeta.readData();
    } catch ( KettleException e ) {
      logger.debug( "Could not read data from repositories meta (typically respositories.xml)." );
    }

    return repositoriesMeta.findRepository( repoName );
  }


  @Override
  public Repository getRepository() {
    if ( this.repository != null ) {
      return this.repository;
    }

    RepositoryMeta repositoryMeta = this.readRepositoryMeta( this.getRepoName() );
    if( repositoryMeta == null ) {
      logger.debug("Repository meta information for repository \"{}\" not found.", this.getRepoName() );
      return null;
    }

    // Load Repository class
    PluginRegistry registry = this.getPluginRegistry();
    Repository repository;
    try {
      repository = registry.loadClass(
        RepositoryPluginType.class,
        repositoryMeta,
        Repository.class
      );
    } catch ( KettlePluginException e ) {
      logger.debug( "Unable to load repository class for {}.", repositoryMeta.getId() );
      return null;
    }

    // Init and connect to repository
    repository.init( repositoryMeta );
    try {
      repository.connect( this.getRepoUsername(), this.getRepoPassword() );
    } catch ( KettleException e ) {
      logger.debug( "Unable to connect to repository \"{}\".", repositoryMeta.getName() );
      return null;
    }

    return this.repository = repository;
  }
  private Repository repository;

  /**
   * Resets the repository so that next time {@link #getRepository()} is called a new repository is returned.
   */
  public void resetRepository() {
    if ( this.repository == null ) {
      return;
    }

    this.repository.disconnect();
    this.repository = null;
  }

}