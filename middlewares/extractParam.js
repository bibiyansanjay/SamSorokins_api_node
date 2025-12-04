/**
 * @method extractParam
 * @memberof module:Middleware
 * @description Middleware to copy a parameter from the request's `params` to `req`.
 */

export default (param) => (req, res, next) => {
  req[param] = req.params[param];
  next();
};
