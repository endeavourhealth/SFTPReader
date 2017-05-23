package org.endeavourhealth.sftpreader.management;

import org.endeavourhealth.sftpreader.Configuration;
import org.endeavourhealth.sftpreader.management.model.Instance;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

@Path("/")
public class ManagementRestEndpoint {

    @GET
    @Path("instances")
    @Produces(MediaType.APPLICATION_JSON)
    public Response test() throws Exception {

        ManagementDataLayer db = getDataLayer();
        List<Instance> instances = db.getInstances();

        return Response
                .ok()
                .entity(instances)
                .build();
    }

    private ManagementDataLayer getDataLayer() throws Exception {
        return new ManagementDataLayer(Configuration.getInstance().getDatabaseConnection());
    }
}
