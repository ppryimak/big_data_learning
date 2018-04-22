package controllers

import javax.inject._
import play.api.Logger
import play.api.libs.ws.WSClient
import play.api.mvc._
import services.HBaseTest;

/**
 * This controller creates an `Action` to handle HTTP requests to the
 * application's home page.
 */
@Singleton
class HomeController @Inject()  (ws: WSClient, hbtest: HBaseTest) extends InjectedController {

  /**
   * Create an Action to render an HTML page with a welcome message.
   * The configuration in the `routes` file means that this method
   * will be called when the application receives a `GET` request with
   * a path of `/`.
   */
  def index = Action {
    Logger.debug("running test: TODO need to be removed in future")
        //TODO: remove this test, when normal hbase connection is ready
    hbtest.test();
    Logger.debug("end running test")
    Ok(views.html.index("Welcome to Full Stack Scala"))
  }

  def time = Action {
    Ok(views.html.index(System.currentTimeMillis().toString))
  }

}
