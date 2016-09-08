package de.fhg.iais.kd.datacron.files.importer.clp;

import java.io.File;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;


/**
 * @author kthellmann
 *
 */
public class CLParameterValidator  implements IParameterValidator {

    @Override
    public void validate(String name, String value) throws ParameterException {
        switch ( name ) {
            case "c":
                File conf = new File(value);

                if ( !conf.isFile() )
                    throw new ParameterException("File " + value + " does not exist.");

                break;

            case "m":

            	 File mapping = new File(value);

                 if ( !mapping.isFile() )
                     throw new ParameterException("File " + value + " does not exist.");

                 break;
                 
            case "o":

           	 File outlier = new File(value);

                if ( !outlier.isFile() )
                    throw new ParameterException("File " + value + " does not exist.");

                break;


            default:
                break;
        }
    }

}
