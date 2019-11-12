var Montage = require('montage/montage');
global.XMLHttpRequest = require('xhr2');

var mainService = require("data/main.datareel/main.mjson").montageObject,
DataOperation = require("montage/data/service/data-operation").DataOperation,
DataStream = require("montage/data/service/data-stream").DataStream,
DataQuery = require("montage/data/model/data-query").DataQuery,
Criteria = require("montage/core/criteria").Criteria,
PhrontCollection = require("phront-data/data/main.datareel/model/collection").Collection,
PhrontImage = require("phront-data/data/main.datareel/model/image").Image,
PhrontPerson = require("phront-data/data/main.datareel/model/person").Person,
PhrontService = require("phront-data/data/main.datareel/model/service").Service,
PhrontProductVariant = require("phront-data/data/main.datareel/model/product-variant").ProductVariant,
phrontProductVariantDescriptor = mainService.objectDescriptorForType(PhrontProductVariant),
PhrontOrganization = require("phront-data/data/main.datareel/model/organization").Organization,
PhrontAddress = require("phront-data/data/main.datareel/model/address").Address,
ShopifyCustomer = require("montage-shopify/data/main.datareel/model/vendor").Vendor,
ShopifyAddress = require("montage-shopify/data/main.datareel/model/address").Address,
ShopifyCollection = require("montage-shopify/data/main.datareel/model/collection").Collection,
phrontOrganizations = [];
// PhrontService = require("phront-data/data/main.datareel/service/phront-service").PhrontService,


var collectionQuery = DataQuery.withTypeAndCriteria(ShopifyCollection),
    customerQuery = DataQuery.withTypeAndCriteria(ShopifyCustomer),
    collectionDataStream,
    phrontDataService = mainService.childServices[0],
    phrontDataServiceTyoes = phrontDataService.types;


function processCollectionProduct(phrontCollection, shopifyProduct, shopifyCollection) {
    var jOriginId,
        phrontService,
        jPhrontServiceCriteria, jPhrontServiceQuery, productPromises,
        phrontCollectionProducts = phrontCollection.products || (phrontCollection.products = []);

    jOriginId = shopifyProduct.identifier.primaryKey;
    //BUG: Here it should be originId, and be mapped to rawData, cutting corner for now:
    jPhrontServiceCriteria = new Criteria().initWithExpression("originId == $.originId", {
        originId: jOriginId
    });
    jPhrontServiceQuery = DataQuery.withTypeAndCriteria(PhrontService, jPhrontServiceCriteria);

    //Product can be shared among collections, fetch it first to see if we already have it:    
    console.log("process shopifyProduct: ",shopifyProduct.title);
    return mainService.fetchData(jPhrontServiceQuery)
    .then(function (result) {        
            if(!result || result.length === 0) {
                console.log("-> Create Phront Product "+shopifyProduct.title);

                //Create, set non-relationship properties and save:
                // phrontService = mainService.createDataObject(serviceDescriptor);
                // phrontService.originId = originId;
                // phrontService.title = shopifyProduct.title;
                // phrontService.description = shopifyProduct.description;
                // phrontService.descriptionHtml = shopifyProduct.descriptionHtml;
                // phrontService.modificationDate = shopifyProduct.createdAt;
                // phrontService.creationDate = shopifyProduct.updatedAt;
                // phrontService.publicationDate = shopifyProduct.publishedAt;
                // phrontService.tags = shopifyProduct.tags;


                return importCollectionProduct(phrontCollection, shopifyProduct, shopifyCollection)
                // return mainService.saveDataObject(phrontService)
                .then(function(aProduct) {
                    if(aProduct) {
                        phrontCollectionProducts.push(aProduct);
                    }
                    return aProduct;
                });
            }
            else {
                //The product already exists, we return it. 
                //console.log("<- Phront Product exists ");
                return result[0];
            }
        },function(error) {
            console.error(error);
        }
    );
}


function importShopifyImage(shopifyImage) {
    if(shopifyImage) {
        //console.log("Importing Shopify Image "+ shopifyImage.originalSrc);
        var imageDescriptor = mainService.objectDescriptorForType(PhrontImage);

        var iPhrontImage = mainService.createDataObject(imageDescriptor);
        iPhrontImage.originId = mainService.dataIdentifierForObject(shopifyImage).primaryKey;
        iPhrontImage.altText = shopifyImage.altText;
        iPhrontImage.originalSrc = shopifyImage.originalSrc;
        iPhrontImage.transformedSrc = shopifyImage.transformedSrc;
        return iPhrontImage;
    }
    return null;
}

function importShopifyProductVariant(shopifyProductVariant, phrontService) {
    if(shopifyProductVariant) {
        var iPhrontProductVariant = mainService.createDataObject(phrontProductVariantDescriptor),
        variantImage, variantImageSavePromise;

        iPhrontProductVariant.originId = shopifyProductVariant.identifier.primaryKey;
        iPhrontProductVariant.title = shopifyProductVariant.title;
        iPhrontProductVariant.availableForSale = shopifyProductVariant.availableForSale;
        iPhrontProductVariant.price = shopifyProductVariant.price;
        iPhrontProductVariant.sku = shopifyProductVariant.sku || null;
        iPhrontProductVariant.weight = shopifyProductVariant.weight;
        iPhrontProductVariant.weightUnit = shopifyProductVariant.weightUnit;
        
        //Does the trigger adds iPhrontProductVariant to phrontService's variants array?
        //How do we stop the cycle? Should the DataService do it since it's outside of the triggers
        //and in-between both?
        //In the end, especially with uuids being stored inline in an array, we need this. It's as useful
        //in the UI as it is when we save.
        iPhrontProductVariant.product = phrontService;
        
        //Image
        if(shopifyProductVariant.image) {
            var variantImage = importShopifyImage(shopifyProductVariant.image);
            variantImageSavePromise = mainService.saveDataObject(variantImage);
        } else {
            variantImageSavePromise = Promise.resolve(null);
        }

        return variantImageSavePromise.then(function(variantImageSaveResolved) {
            if(variantImage) {
                iPhrontProductVariant.images = [variantImage];
            } else {
                iPhrontProductVariant.images = null;
            }
            //selectedOptions
            /*
                selectedOptions are like this:
                "selectedOptions": [
                    {
                    "name": "Durée",
                    "value": "120"
                    },
                    {
                    "name": "Bienfaits",
                    "value": "Redéfinissant"
                    },
                    {
                    "name": "Partie du corps",
                    "value": "Lèvres"
                    }
                ]
            */
            iPhrontProductVariant.selectedOptions = shopifyProductVariant.selectedOptions;

            /*
                Presentment Prices
                compare_at_price	
                    "compare_at_price": "299.00"
                    The original price of the item before an adjustment or a sale.
            */
            iPhrontProductVariant.presentmentPrices = shopifyProductVariant.presentmentPrices;

            return iPhrontProductVariant;

        },function(variantImageSaveRejected) {
            console.error("variantImageSaveRejected:",variantImageSaveRejected);
            return Promise.resolve(null);
        })

    }
    else return Promise.resolve(null);
}

function importCollectionProduct(phrontCollection, shopifyProduct, shopifyCollection) {
    var phrontService,
        originId = shopifyProduct.identifier.primaryKey,
        serviceDescriptor = mainService.objectDescriptorForType(PhrontService),
        shopifyProductCollections, phrontProductCollections, iShopifyProductCollection,
        shopifyProductImages, phrontProductImages,
        shopifyProductVariants, phrontProductVariants, iShopifyProductVariant, iPhrontProductVariant, iPhrontProductVariantPromise,
        i, countI, iImage, iImageSavePromises, iImageSavePromise,
        iPhrontServiceVariantsSavePromises,iPhrontServiceVariantsSavePromise,
        phrontServiceDataStream;
    //Product can be shared among collections, fetch it first to see if we already have it:


    console.log("Importing shopifyProduct[",shopifyProduct.title+"]");

    //Create, set non-relationship properties and save:
    phrontService = mainService.createDataObject(serviceDescriptor);
    phrontService.originId = originId;
    phrontService.title = shopifyProduct.title;
    phrontService.description = shopifyProduct.description;
    phrontService.descriptionHtml = shopifyProduct.descriptionHtml;
    phrontService.modificationDate = shopifyProduct.createdAt;
    phrontService.creationDate = shopifyProduct.updatedAt;
    phrontService.publicationDate = shopifyProduct.publishedAt;
    phrontService.tags = shopifyProduct.tags;

    return mainService.saveDataObject(phrontService)
    .then(function(createCompletedOperation) {
        //Images
        shopifyProductImages = shopifyProduct.images;
        phrontProductImages = null;
        if(shopifyProductImages && shopifyProductImages.length) {
            iImageSavePromises = [];
            for(i=0, countI=shopifyProductImages.length;i<countI;i++) {
                iImage = importShopifyImage(shopifyProductImages[i]);
                if(iImage) {
                    (phrontProductImages || (phrontProductImages = [])).push(iImage);
                    iImageSavePromises.push(mainService.saveDataObject(iImage));
                }
            }
            if(iImageSavePromises.length) {
                return Promise.all(iImageSavePromises);
            }
            else return Promise.resolve(null);

        }
        else {
            return Promise.resolve(null);
        }
    }, function(createFailedOperation) {
        console.error(createFailedOperation);
    })
    .then(function(ImageCreateCompletedOperations) {
        if(!phrontProductImages || phrontProductImages.lengh === 0) {
            console.log("Product[",shopifyProduct.title+"] has no image");
            return Promise.resolve({data:phrontService});
        }
        else {
            // console.log("Product[",shopifyProduct.title+"] Images saved",ImageCreateCompletedOperations);

            phrontService.images = phrontProductImages;
            return mainService.saveDataObject(phrontService);
        }

    }, function(createFailedOperation) {

    })
    .then(function(ImagesSavedCompletedOperations) {

        //Vendor: 
        var vendorObject = shopifyProduct.vendor,
            vendorObjectName = vendorObject ? vendorObject.vendor : null,
            vendorName = shopifyProduct.vendorName,
            vendorNameCondition = (vendorObjectName || vendorName).trim();

        if(!vendorNameCondition) {
            console.error("No way to find a vendor for shopifyProduct "+shopifyProduct.title);
        }

        phrontOrganizationNameCriteria = new Criteria().initWithExpression("name == $", vendorNameCondition);
            organizationNamedQuery = DataQuery.withTypeAndCriteria(PhrontOrganization, phrontOrganizationNameCriteria);

        return mainService.fetchData(organizationNamedQuery)
            .then(function (result) { 
                 if(result && result.length) {
                     var vendorOrganization = result[0];
                     phrontService.vendors = [vendorOrganization];
                     return mainService.saveDataObject(phrontService);
                 }  
                 else {
                     phrontService.vendors = null;
                     console.error("!!! No organization found with name equal to "+vendorNameCondition+" for Service ["+shopifyProduct.title+"]");

                     return null;
                 }    
        
            },function(error) {
                console.log(error);
            });

    }, function(imagesSaveFailed) {
        console.log("Product[",shopifyProduct.title+"] Images save failed",imagesSaveFailed);
    })
    .then(function(vendorSaveCompleted) {
        if(phrontService.vendors && phrontService.vendors.length > 0) {
            //console.log("Product[",shopifyProduct.title+"] Vendor["+phrontService.vendors[0].name+"] Save Completed");
        }
        // else {
        //     console.error("!!! NO VENDOR FOUND for Product[",shopifyProduct.title+"]",vendorSaveCompleted);
        // }

        //Variants
        shopifyProductVariants = shopifyProduct.variants;
        if(shopifyProductVariants && shopifyProductVariants.length) {
            phrontProductVariants = [];
            iPhrontServiceVariantsSavePromises = [];
            for(i=0, countI=shopifyProductVariants.length;i<countI;i++) {
                iShopifyProductVariant = shopifyProductVariants[i];
                iPhrontProductVariantPromise = importShopifyProductVariant(iShopifyProductVariant, phrontService);
                iPhrontServiceVariantsSavePromises.push(

                    iPhrontProductVariantPromise
                    .then(function(iPhrontProductVariant) {
                        phrontProductVariants.push(iPhrontProductVariant);
                        return mainService.saveDataObject(iPhrontProductVariant)
                    }, function(error) {
                        console.error("importShopifyProductVariant error",error);
                    })
                );
            } 
            
            return Promise.all(iPhrontServiceVariantsSavePromises);
        }
        else {
            phrontProductVariants = null;
            return Promise.resolve(null);
        }

    }, function(vendorSaveFailed) {
        console.log("Product[",shopifyProduct.title+"] Vendor["+phrontService.vendors[0].name+"] save failed",vendorSaveFailed);
    })
    .then(function(productVariantsSaveCompleted) {
        // console.log("Product[",shopifyProduct.title+"] Variants["+phrontService.variants+"] saved",productVariantsSaveCompleted);

        phrontService.variants = phrontProductVariants;
        return mainService.saveDataObject(phrontService);

    }, function(productVariantsSaveFailed) {
        console.log("Product[",shopifyProduct.title+"] Variants["+phrontService.variants+"] save failed",productVariantsSaveFailed);
    })
    .then(function(phrontServiceSaveCompleted) {

        console.log("Imported shopifyProduct: ",shopifyProduct.title);
        return phrontService;

    }, function(phrontServiceSaveFailed) {
        console.log("phrontServiceSaveFailed",phrontServiceSaveFailed)
    })

}


function importCollection(shopifyCollection) {
    var phrontCollectionDescriptor = mainService.objectDescriptorForType(PhrontCollection),
    imageDescriptor = mainService.objectDescriptorForType(PhrontImage),
    iPhrontCollection, iShopifyImage, iPhrontImage,
    originId,
    phrontCriteria, phrontQuery;
    console.log("Importing shopifyCollection:",shopifyCollection.title);

    // //1-offf: Alter table to add oId column that reference the primary key coming from the origin:
    // var updateObjectDescriptorOperation = new DataOperation();
    // updateObjectDescriptorOperation.type = DataOperation.Type.Update;
    // updateObjectDescriptorOperation.criteria = //What's the criteria representing the Collection ObjectDescriptor?;
    // //Start with the model? What's the Collection ObjectDescriptor's data Identifier?
    // updateObjectDescriptorOperation.data = {
        
    // };
    // phrontService.handleUpdateOperation(iOperation);

    originId = shopifyCollection.identifier.primaryKey;
    //BUG: Here it should be originId, and be mapped to rawData, cutting corner for now:
    phrontCriteria = new Criteria().initWithExpression("originId == $.originId", {
        originId: originId
    });
    phrontQuery = DataQuery.withTypeAndCriteria(PhrontCollection, phrontCriteria);

    //fetch it first to see if we already have it:    
    console.log("process shopifyCollection: ",shopifyCollection.title);
    return mainService.fetchData(phrontQuery)
    .then(function (result) {        
            if(!result || result.length === 0) {
                //Create a new PhrontCollection
                console.log("Creating Phront Collection "+ shopifyCollection.title);
                iPhrontCollection = mainService.createDataObject(phrontCollectionDescriptor);
                iPhrontCollection.originId = mainService.dataIdentifierForObject(shopifyCollection).primaryKey;
                iPhrontCollection.title = shopifyCollection.title;
                iPhrontCollection.description = shopifyCollection.description;
                iPhrontCollection.descriptionHtml = shopifyCollection.descriptionHtml;
                iPhrontCollection.products = null;
                iPhrontCollection.image = null;

                return mainService.saveDataObject(iPhrontCollection)
                .then(function(createCompletedOperation) {
                    return createCompletedOperation.data
                },function(error) {
                    console.error(error);
                });
            }
            else {
                //The product already exists, we return it. 
                console.log("Phront Collection "+ shopifyCollection.title + " exists ");
                iPhrontCollection = result[0];
                return iPhrontCollection;
            }
        },function(error) {
            console.error(error);
    })
    .then(function(iPhrontCollection) {
        //Collection saved
        //Create Collection's Image
        iShopifyImage = shopifyCollection.image;
        iPhrontImage = iPhrontCollection.image;
        if(iShopifyImage && iPhrontImage) {
            if(iShopifyImage.originalSrc === iPhrontImage.originalSrc &&
                iShopifyImage.altText === iPhrontImage.altText && 
                iShopifyImage.transformedSrc === iPhrontImage.transformedSrc) {
                return Promise.resolve({data:iPhrontCollection});
            } else {
                iPhrontImage.originalSrc = iShopifyImage.originalSrc;
                iPhrontImage.altText = iShopifyImage.altText;
                iPhrontImage.transformedSrc = iShopifyImage.transformedSrc;
                return mainService.saveDataObject(iPhrontImage)
            }
        }
        else if(iShopifyImage && !iPhrontImage) {
            console.log("Importing Shopify Collection ",shopifyCollection.title+"'s image "+iShopifyImage.originalSrc);

            // iPhrontImage = mainService.createDataObject(imageDescriptor);
            // iPhrontImage.originId = mainService.dataIdentifierForObject(iShopifyImage).primaryKey;
            // iPhrontImage.altText = iShopifyImage.altText;
            // iPhrontImage.originalSrc = iShopifyImage.originalSrc;
            // iPhrontImage.transformedSrc = iShopifyImage.transformedSrc;
            iPhrontImage = importShopifyImage(iShopifyImage);
            return mainService.saveDataObject(iPhrontImage)
            .then(function(createCompletedOperation) {
                var createdImage = createCompletedOperation.data;
                //Now it's saved, check it's dataIdentifier
                //var iPhrontImageIdentifer =  mainService.dataIdentifierForObject(createdImage);
                //Save assigment of image
                iPhrontCollection.image = createdImage;
                return mainService.saveDataObject(iPhrontCollection);
            },function(createFailedOperation) {
                console.log("createFailedOperation:",createFailedOperation);
            });
        }
        else {
            return Promise.resolve({data:iPhrontCollection});
        }

    },function(createFailedOperation) {

    })
    .then(function(imageUpdatedCompletedOperation) {
        var iPhrontCollection = imageUpdatedCompletedOperation.data,
            iShopifyProducts, j, countJ, jShopifyProduct;

        //Create Collection's Products
        iShopifyProducts = shopifyCollection.products;
        if(!iShopifyProducts) return Promise.resolve([]);

        console.log("Importing shopifyCollection: "+shopifyCollection.title+", "+iShopifyProducts.length+" products");

        productPromises = [];
        for(j=0, countJ = iShopifyProducts.length;j<countJ;j++) {
            jShopifyProduct = iShopifyProducts[j];
            productPromises.push(processCollectionProduct(iPhrontCollection, jShopifyProduct, shopifyCollection));
        }

        return Promise.all(productPromises);
    },function(imageUpdatedFailedOperation) {
        console.error("Image update failed:",imageUpdatedFailedOperation);
    })
    .then(function(Importedproducts) {
        if(Importedproducts.length) {
            console.log("Saving Phront Collection "+iPhrontCollection.title+", "+Importedproducts.length+" products");

            iPhrontCollection.products = Importedproducts;
            //Saving collection's products
            return mainService.saveDataObject(iPhrontCollection)
        }
        else {
            console.log("No products for Phront Collection: "+iPhrontCollection.title);

            //Fake an operation for now to pass iPhrontCollection to next step
            return Promise.resolve({data:iPhrontCollection});
        }
    },function(error) {
        console.error("product import error:",error);
    })
    .then(function(collectionUpdatedCompletedOperation) {
        var iPhrontCollection = collectionUpdatedCompletedOperation.data,
            products = iPhrontCollection.products,
            i, iProduct, iProductCollections, countI,
            productCollectionSavePromise;

        if(!products) {
            //Clunky, next step expects an operation to be resolved.
            return Promise.resolve([]);
        }

        if(countI = products.length) {
            productCollectionSavePromise = [];
        }
        
        /*
            Now we need to make sure the products.collections property also contains iPhrontCollection. But since we may not have the full relationship fetched,
            we can test locally if not there, cheap, and if not add it.

            The SQL updating the array containing the relationship will test again.
            But right now we don't have observing on the arrays for data purpose.
            The trigger needs to observe the arrays are they are being set. So we could
            use the current observing, and then the trigger dispatch the change event, or
            implement listen with events right away.
        */
        
        for(i=0;i<countI;i++) {
            iProduct = products[i];
            productCollectionSavePromise.push(updateProductCollections(iProduct, iPhrontCollection));    
        }

        return Promise.all(productCollectionSavePromise);

    },function(collectionUpdatedFailedOperation) {
        console.error("collectionUpdatedFailedOperation:",collectionUpdatedFailedOperation);
    })
    .then(function(productSaveOperationSucceeded) {
        console.log("Imported iPhrontCollection: "+iPhrontCollection.title+", which has "+ (iPhrontCollection.products? iPhrontCollection.products.length : 0)) + " products";
    },function(productSaveOperationFailed) {

    });

}

function updateProductCollections(iProduct, iPhrontCollection) {
    return mainService.getObjectProperties(iProduct, "collections").then(function () {
        iProductCollections = iProduct.collections;
        if(!iProductCollections) {
            iProductCollections = [iPhrontCollection];
            iProduct.collections = iProductCollections;
            //Save iProduct's collection 
            return mainService.saveDataObject(iProduct);
        }
        else if(iProductCollections.indexOf(iPhrontCollection) === -1) {
            //Don't see it locally, add it:
            //We need listening on array changes for this to work
            iProductCollections.push(iPhrontCollection);
            //Save iProduct's collection 
            return mainService.saveDataObject(iProduct);
        }
        //The product already has that collection
        else {
            //The two others create an operation, should we have a "no-op" for cases like this?
            return Promise.resolve(true);
        }

    });

}

function importCustomerAsOrganization(iCustomer) {
    var iPhrontOrganization, person, phrontImage, phrontImageSavedPromise, phrontPersonSavedPromise, iShopifyCustomerAddresses, iPhrontOrganizationAddresses,
    j, countJ, jShopifyAddress, jPhrontAddress, jPhrontAddressCreationPromises=[], jPhrontAddressCreationPromise;

    console.log("--> Importing Customer "+iCustomer.firstName+" "+iCustomer.lastName);
    iPhrontOrganization = mainService.createDataObject(PhrontOrganization);
    iPhrontOrganization.originId = iCustomer.identifier.primaryKey;
    /*
        If this isn't set, durig mapObjectToRawData, 

            result = this.service.rootService.getObjectPropertyExpressions(object, requiredObjectProperties);

        is called, which ends-up triggering a fetch by a Trigger that has no business being done there.

        We might need to dissociate keys that are requisitePropertyNames for fetch from the ones that would
        be required to save, which should really be expressed in the model.
    */

    iPhrontOrganization.name = organizationNameFromShopifyCustomer(iCustomer);

    if( (iCustomer.tags.indexOf("Santé") !== -1) && iCustomer.addresses[0] && iCustomer.addresses[0].company ) {
        iPhrontOrganization.type = iCustomer.addresses[0].company;  
    }
    iPhrontOrganization.parent = null;

    iPhrontOrganization.email = iCustomer.email;
    iPhrontOrganization.phone = iCustomer.phone;
    iPhrontOrganization.tags = iCustomer.tags;


    //Image
    if(iCustomer.image) {
        phrontImage = importShopifyImage(iCustomer.image);
        phrontImageSavedPromise = mainService.saveDataObject(phrontImage);
    }
    else {
        phrontImageSavedPromise = Promise.resolve(null);
    }

    return phrontImageSavedPromise.then(function(createImageCompleted) {
        //Check if we should create a person.
        if(iCustomer.firstName && iCustomer.lastName) {
            person = mainService.createDataObject(PhrontPerson);
            person.firstName = iCustomer.firstName;
            person.lastName = iCustomer.lastName;
            person.email = iCustomer.email;
            person.phone = iCustomer.phone;
            person.tags = iCustomer.tags;
            person.image = iCustomer.image || null;

            phrontPersonSavedPromise = mainService.saveDataObject(person);
        }
        else {
            phrontPersonSavedPromise = Promise.resolve(null);
        }
        return phrontPersonSavedPromise;
        
    },function(createImageFailed) {
        console.error(createImageFailed);
    })
    .then(function(createPersonCompleted) {

        //Main Contact, a People instance
        //Or a humanResources array, with a Resource Object, with a role, title, pro-address, compensation, etc...
        iPhrontOrganization.mainContact = person ? person : null;

        //Assign image to organization and person
        iPhrontOrganization.images = phrontImage ? [phrontImage] : null;
        
         //Addresses
        iShopifyCustomerAddresses = iCustomer.addresses;
        if(iShopifyCustomerAddresses && iShopifyCustomerAddresses.length > 0) {
            iPhrontOrganizationAddresses = [];

            for(j=0, countJ = iShopifyCustomerAddresses.length;(j<countJ);j++) {
                jShopifyAddress = iShopifyCustomerAddresses[j];
                jPhrontAddress = mainService.createDataObject(PhrontAddress);
                jPhrontAddress.originId = jShopifyAddress.identifier.primaryKey;
                jPhrontAddress.name = jShopifyAddress.name;
                jPhrontAddress.firstName = jShopifyAddress.firstName;
                jPhrontAddress.lastName = jShopifyAddress.lastName;
                jPhrontAddress.phone = jShopifyAddress.phone;
                jPhrontAddress.address1 = jShopifyAddress.address1;
                jPhrontAddress.address2 = jShopifyAddress.address2;
                jPhrontAddress.city = jShopifyAddress.city;
                jPhrontAddress.provinceCode = jShopifyAddress.provinceCode;
                jPhrontAddress.zip = jShopifyAddress.zip;
                jPhrontAddress.country = jShopifyAddress.country;
                jPhrontAddress.latitude = jShopifyAddress.latitude;
                jPhrontAddress.longitude = jShopifyAddress.longitude;
                jPhrontAddressCreationPromises.push(mainService.saveDataObject(jPhrontAddress)
                .then(function(createCompletedOperation) {
                    iPhrontOrganizationAddresses.push(jPhrontAddress);
                },
                function(createFailedOperation) {
                    console.error("Error Processessing Cutomers Batch "+readUpdatedOperation.batchCount,error);
                }));

            }
            jPhrontAddressCreationPromise = Promise.all(jPhrontAddressCreationPromises);
        }
        else {
            if(!iPhrontOrganization.name) {
                iPhrontOrganization.name = iCustomer.firstName;
            }
            jPhrontAddressCreationPromise = Promise.resolve(true);
        }

        return jPhrontAddressCreationPromise;

    },function(createPersonFailed) {
        console.error(createPersonFailed);
    })
    //With all Organization's addresses saved
    .then(function(addressCreationCompletedOperations) {
        iPhrontOrganization.addresses = iPhrontOrganizationAddresses;
        return mainService.saveDataObject(iPhrontOrganization);
    },
    function(error) {
        console.error("Error Processessing Customers Batch "+readUpdatedOperation.batchCount+" Addresses:",error);
    })
    .then(function(organizationCreationCompletedOperation) {
        //console.log("<-- Importing Customer "+iCustomer.firstName+" "+iCustomer.lastName);
        return Promise.resolve(iPhrontOrganization);
    },function(organizationCreationFailedOperation) {
        return Promise.reject(iPhrontOrganization);
        console.error(createPersonFailed);
    });

}

function organizationNameFromShopifyCustomer(shopifyCustomer) {
    var addresses = shopifyCustomer.addresses,
        jShopifyAddress,
        organizationName,
        shopifyCustomerFullName = shopifyCustomer.firstName.trim()+" "+shopifyCustomer.lastName.trim(),
        jShopifyAddressFullName ;

     //We're going to lookup the company name only in the first address.
    if(addresses && addresses.length > 0) {
        jShopifyAddress = addresses[0];
        jShopifyAddressFullName = jShopifyAddress.firstName.trim()+" "+jShopifyAddress.lastName.trim();
    }
    if(shopifyCustomer.tags.indexOf("Santé") !== -1) {
        //Name is "firstName lastName"
        if(jShopifyAddressFullName) {
            organizationName = jShopifyAddressFullName;
        }
        else if(jShopifyAddress) {
            organizationName = jShopifyAddress.company.trim();                           
        }
        else {
            organizationName = shopifyCustomerFullName;
        }
    }
    else if(jShopifyAddress && jShopifyAddress.company) {
        organizationName = jShopifyAddress.company.trim();
    }
    //We haven't used last name for customers who are companies, except for
    //Santé professionals
    else if(jShopifyAddress && jShopifyAddress.firstName) {
        if(!jShopifyAddress.lastName) {
            organizationName = jShopifyAddress.firstName.trim();
        }
        else {
            organizationName = jShopifyAddressFullName;
        }
    }
    else {
        console.error("Company misssing for customer in both company and firstName field:",shopifyCustomer,", address:",jShopifyAddress);
    }
    return organizationName.trim();
}

function processShopifyCustomer(shopifyCustomer) {
    var originId,
    phrontCriteria, phrontQuery, productPromises;

    originId = shopifyCustomer.identifier.primaryKey;
    //BUG: Here it should be originId, and be mapped to rawData, cutting corner for now:
    phrontCriteria = new Criteria().initWithExpression("originId == $.originId", {
        originId: originId
    });
    phrontQuery = DataQuery.withTypeAndCriteria(PhrontOrganization, phrontCriteria);

    console.log("--> Processing Customer "+shopifyCustomer.firstName+" "+shopifyCustomer.lastName);
    return mainService.fetchData(phrontQuery)
    .then(function (result) {        
            if(!result || result.length === 0) {
                // console.log("-> Import Phront Product "+shopifyProduct.title);
                return importCustomerAsOrganization(shopifyCustomer);
            }
            else {
                //The organization already exists, we return it. 
                // console.log("<- Phront Organization exists ");
                return Promise.resolve(result[0]);
            }
        },function(error) {
            console.error(error);
        }
    );
}
/*
        phrontOrganizationNameCriteria = new Criteria().initWithExpression("name == $", vendorNameCondition);
            organizationNamedQuery = DataQuery.withTypeAndCriteria(PhrontOrganization, phrontOrganizationNameCriteria);

        return mainService.fetchData(organizationNamedQuery)
            .then(function (result) { 
*/

mainService.fetchData(customerQuery).thenForEach(function(readUpdatedOperation) {
    var shopifyCustomerBatch = readUpdatedOperation.data,
        i, countI, iCustomer, iPhrontOrganization, iShopifyCustomerAddresses, iPhrontOrganizationAddresses,
        j, countJ, jShopifyAddress, jPhrontAddress, jPhrontAddressCreationPromises=[], jPhrontAddressCreationPromise,
        jPhrontOrganizationCreationPromises=[], jPhrontOrganizationCreationPromise;

    console.log("Processing Cutomers Batch ", readUpdatedOperation.batchCount);

    for(i=0, countI = shopifyCustomerBatch.length;(i<countI);i++) {
        iCustomer = shopifyCustomerBatch[i];
        jPhrontOrganizationCreationPromises.push(processShopifyCustomer(iCustomer));
    }

    return Promise.all(jPhrontOrganizationCreationPromises).then(function(creationCompletedOperations) {
        console.log("Processed Cutomers Batch ", readUpdatedOperation.batchCount);
        return creationCompletedOperations;
    },
    function(creationFailedOperations) {
        console.error("Error Processessing Customers Batch "+creationFailedOperations);
    });

    
})
.then(function(organizations) {
    collectionDataStream = mainService.fetchData(collectionQuery).then(
        function (shopifyCollections) {
            var collectionPromises = [];
            //console.log(shopifyCollections);
    
            for(var i=0, countI = shopifyCollections.length;i<countI;i++) {
                collectionPromises.push(importCollection(shopifyCollections[i]));
            }

            Promise.all(collectionPromises)
            .then(function(collections) {
                console.log("All Collections Processed");
            });
    
            //More to do to get there!
            //mainService.saveChanges();
    
        }
    );    
},
function(error){

});
    





//HYBRID
// // Execute
// var mrRequire = require('mr/bootstrap-node');
// var PATH = require("path");
// mrRequire.loadPackage(PATH.join(__dirname, "."))
// // // Preload montage to avoid montage-testing/montage to be loaded
// .then(function (mr) {
//     return mr.async('montage').then(function (montage) {
//          return mr;
//     });
// })
// .then(function (mr) {
//     return mr.async("all");
// }).then(function () {
//     console.log('Done');
//     process.exit(exitCode);
// }).thenReturn();


// // Execute
// var mrRequire = require('mr/bootstrap-node');
// var PATH = require("path");
// mrRequire.loadPackage(PATH.join(__dirname, ".")).then(function (mr) {
//     return mr.async("all");
// }).then(function () {
//     console.log('Done');
//     process.exit(exitCode);
// }).thenReturn();