/**
 * 
 */
package edu.ucsd.forward.exception;

/**
 * Support for location information.
 * 
 * @author Michalis Petropoulos
 * 
 */
public interface LocationSupport
{
    /**
     * Gets the location.
     * 
     * @return the location.
     */
    public Location getLocation();
    
    /**
     * Sets the location.
     * 
     * @param location
     *            the location.
     */
    public void setLocation(Location location);
    
}
